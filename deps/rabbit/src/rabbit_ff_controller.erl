%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @author The RabbitMQ team
%% @copyright 2022 VMware, Inc. or its affiliates.
%%
%% @doc
%% The feature flag controller is responsible for synchronization and managing
%% concurrency when feature flag states are changed.
%%
%% It makes sure only one node can enable feature flags or synchronize its
%% feature flag states with other nodes at a time. If another node wants to
%% enable a feature flag or if it joins a cluster and needs to synchronize its
%% feature flag states, it will wait for the current task to finish.
%%
%% The feature flag controller is used as soon as the `feature_flags_v2'
%% feature flag is enabled.
%%
%% Compared to the initial subsystem, it also introduces a new migration
%% function signature; See {@link rabbit_feature_flags:migration_fun_v2()}.

-module(rabbit_ff_controller).
-behaviour(gen_statem).

-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/assert.hrl").

-include_lib("rabbit_common/include/logging.hrl").

-export([is_supported/1, is_supported/2,
         enable/1,
         check_node_compatibility/1,
         sync_cluster/0,
         refresh_after_app_load/0]).

%% Internal use only.
-export([start/0,
         start_link/0,
         rpc_call/5,
         all_nodes/0,
         running_nodes/0]).

%% gen_statem callbacks.
-export([callback_mode/0,
         init/1,
         terminate/3,
         code_change/4,

         standing_by/3,
         waiting_for_end_of_controller_task/3,
         updating_feature_flag_states/3]).

-record(?MODULE, {from,
                  notify = #{}}).

-define(LOCAL_NAME, ?MODULE).
-define(GLOBAL_NAME, {?MODULE, global}).

%% Default timeout for operations on remote nodes.
-define(TIMEOUT, 60000).

start() ->
    gen_statem:start({local, ?LOCAL_NAME}, ?MODULE, none, []).

start_link() ->
    gen_statem:start_link({local, ?LOCAL_NAME}, ?MODULE, none, []).

is_supported(FeatureNames) ->
    is_supported(FeatureNames, ?TIMEOUT).

is_supported(FeatureName, Timeout) when is_atom(FeatureName) ->
    is_supported([FeatureName], Timeout);
is_supported(FeatureNames, Timeout) when is_list(FeatureNames) ->
    Nodes = running_nodes(),
    case collect_inventory_on_nodes(Nodes, Timeout) of
        {ok, Inventory} ->
            lists:all(
              fun(FeatureName) ->
                      is_known_and_supported(Inventory, FeatureName)
              end,
              FeatureNames);
        _Error ->
            false
    end.

enable(FeatureName) when is_atom(FeatureName) ->
    enable([FeatureName]);
enable(FeatureNames) when is_list(FeatureNames) ->
    ?LOG_DEBUG(
       "Feature flags: REQUEST TO ENABLE: ~p",
       [FeatureNames],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, {enable, FeatureNames}).

check_node_compatibility(RemoteNode) ->
    ThisNode = node(),
    ?LOG_DEBUG(
       "Feature flags: CHECKING COMPATIBILITY between nodes `~s` and `~s`",
       [ThisNode, RemoteNode],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    %% We don't go through the controller process to check nodes compatibility
    %% because this function is used while `rabbit' is stopped usually.
    %%
    %% There is no benefit in starting a controller just for this check
    %% because it would not guaranty that the compatibility remains true after
    %% this function finishes and before the node starts and synchronizes
    %% feature flags.
    check_node_compatibility_task(ThisNode, RemoteNode).

sync_cluster() ->
    ?LOG_DEBUG(
       "Feature flags: SYNCING FEATURE FLAGS in cluster...",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    case erlang:whereis(?LOCAL_NAME) of
        Pid when is_pid(Pid) ->
            %% The function is called while `rabbit' is running.
            gen_statem:call(?LOCAL_NAME, sync_cluster);
        undefined ->
            %% The function is called while `rabbit' is stopped. We need to
            %% start a one-off controller, again to make sure concurrent
            %% changes are blocked.
            {ok, Pid} = start_link(),
            Ret = gen_statem:call(Pid, sync_cluster),
            gen_statem:stop(Pid),
            Ret
    end.

refresh_after_app_load() ->
    ?LOG_DEBUG(
       "Feature flags: REFRESHING after applications load...",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:call(?LOCAL_NAME, refresh_after_app_load).

%% --------------------------------------------------------------------
%% gen_statem callbacks.
%% --------------------------------------------------------------------

callback_mode() ->
    state_functions.

init(_Args) ->
    {ok, standing_by, none}.

standing_by(
  {call, From} = EventType, EventContent, none)
  when EventContent =/= notify_when_done ->
    ?LOG_DEBUG(
       "Feature flags: registering controller globally before "
       "proceeding with task: ~p",
       [EventContent],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    %% The first step is to register this process globally (it is already
    %% registered locally). The purpose is to make sure this one takes full
    %% control on feature flag changes among other controllers.
    %%
    %% This is useful for situations where a new node joins the cluster while
    %% a feature flag is being enabled. In this case, when that new node joins
    %% and its controller wants to synchronize feature flags, it will block
    %% and wait for this one to finish.
    case register_globally() of
        yes ->
            %% We would register the process globally. Therefore we can
            %% proceed with enabling/syncing feature flags.
            ?LOG_DEBUG(
               "Feature flags: controller globally registered; can proceed "
               "with task",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            Data = #?MODULE{from = From},
            {next_state, updating_feature_flag_states, Data,
             [{next_event, internal, EventContent}]};

        no ->
            %% Another controller is globally registered. We ask that global
            %% controller to notify us when it is done, and we wait for its
            %% response.
            ?LOG_DEBUG(
               "Feature flags: controller NOT globally registered; need to "
               "wait for the current global controller's task to finish",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            RequestId = notify_me_when_done(),
            {next_state, waiting_for_end_of_controller_task, RequestId,
             [{next_event, EventType, EventContent}]}
    end;
standing_by(
  {call, From}, notify_when_done, none) ->
    %% This state is entered when a globally-registered controller finished
    %% its task but had unhandled `notify_when_done` requests in its inbox. We
    %% just need to notify the caller that it can proceed.
    notify_waiting_controller(From),
    {keep_state_and_data, []}.

waiting_for_end_of_controller_task(
  {call, _From}, _EventContent, _RequestId) ->
    {keep_state_and_data, [postpone]};
waiting_for_end_of_controller_task(
  info, Msg, RequestId) ->
    case gen_statem:check_response(Msg, RequestId) of
        {reply, done} ->
            ?LOG_DEBUG(
               "Feature flags: current global controller's task finished; "
               "trying to take next turn",
               [],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        {error, Reason} ->
            ?LOG_DEBUG(
               "Feature flags: error while waiting for current global "
               "controller's task: ~0p; trying to take next turn",
               [Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {next_state, standing_by, none, []};
        no_reply ->
            ?LOG_DEBUG(
               "Feature flags: unknown message while waiting for current "
               "global controller's task: ~0p; still waiting",
               [Msg],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            keep_state_and_data
    end.

updating_feature_flag_states(
  internal, Task, #?MODULE{from = From} = Data) ->
    Reply = proceed_with_task(Task),

    ?LOG_DEBUG(
       "Feature flags: unregistering controller globally",
       [],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    unregister_globally(),
    notify_waiting_controllers(Data),
    {next_state, standing_by, none, [{reply, From, Reply}]};
updating_feature_flag_states(
  {call, From}, notify_when_done, #?MODULE{notify = Notify} = Data) ->
    Notify1 = Notify#{From => true},
    Data1 = Data#?MODULE{notify = Notify1},
    {keep_state, Data1}.

proceed_with_task({enable, FeatureNames}) ->
    enable_task(FeatureNames);
proceed_with_task(sync_cluster) ->
    sync_cluster_task();
proceed_with_task(refresh_after_app_load) ->
    refresh_after_app_load_task().

terminate(_Reason, _State, _Data) ->
    ok.

code_change(_OldVsn, OldState, OldData, _Extra) ->
    {ok, OldState, OldData}.

%% --------------------------------------------------------------------
%% Global name registration.
%% --------------------------------------------------------------------

register_globally() ->
    ?LOG_DEBUG(
       "Feature flags: [global sync] @ ~s",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    ok = rabbit_node_monitor:global_sync(),
    ?LOG_DEBUG(
       "Feature flags: [global register] @ ~s",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    global:register_name(?GLOBAL_NAME, self()).

unregister_globally() ->
    ?LOG_DEBUG(
       "Feature flags: [global unregister] @ ~s",
       [node()],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    _ = global:unregister_name(?GLOBAL_NAME),
    ok.

notify_me_when_done() ->
    gen_statem:send_request({global, ?GLOBAL_NAME}, notify_when_done).

notify_waiting_controllers(#?MODULE{notify = Notify}) ->
    maps:fold(
      fun(From, true, Acc) ->
              notify_waiting_controller(From),
              Acc
      end, ok, Notify).

notify_waiting_controller({ControlerPid, _} = From) ->
    ControlerNode = node(ControlerPid),
    ?LOG_DEBUG(
       "Feature flags: controller's task finished; notify waiting controller "
       "on node ~p",
       [ControlerNode],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    gen_statem:reply(From, done).

%% --------------------------------------------------------------------
%% Code to check compatibility between nodes.
%% --------------------------------------------------------------------

-spec check_node_compatibility_task(Node, Node) -> Ret when
      Node :: node(),
      Ret :: ok | {error, Reason},
      Reason :: incompatible_feature_flags.

check_node_compatibility_task(NodeA, NodeB) ->
    ?LOG_NOTICE(
       "Feature flags: checking nodes `~s` and `~s` compatibility...",
       [NodeA, NodeB],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    NodesA = list_nodes_clustered_with(NodeA),
    NodesB = list_nodes_clustered_with(NodeB),
    AreCompatible = case collect_inventory_on_nodes(NodesA) of
                        {ok, InventoryA} ->
                            ?LOG_DEBUG(
                               "Feature flags: inventory of node `~s`:~n~p",
                               [NodeA, InventoryA],
                               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                            case collect_inventory_on_nodes(NodesB) of
                                {ok, InventoryB} ->
                                    ?LOG_DEBUG(
                                       "Feature flags: inventory of node "
                                       "`~s`:~n~p",
                                       [NodeB, InventoryB],
                                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                                    are_compatible(InventoryA, InventoryB);
                                _ ->
                                    false
                            end;
                        _ ->
                            false
                    end,
    case AreCompatible of
        true ->
            ?LOG_NOTICE(
               "Feature flags: nodes `~s` and `~s` are compatible",
               [NodeA, NodeB],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            ok;
        false ->
            ?LOG_WARNING(
               "Feature flags: nodes `~s` and `~s` are incompatible",
               [NodeA, NodeB],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, incompatible_feature_flags}
    end.

-spec list_nodes_clustered_with(Node) -> [Node] when
      Node :: node().

list_nodes_clustered_with(Node) ->
    %% If Mnesia is stopped on the given node, it will return an empty list.
    %% In this case, only consider that stopped node.
    case rpc_call(Node, ?MODULE, running_nodes, [], ?TIMEOUT) of
        []   -> [Node];
        List -> List
    end.

-spec are_compatible(Inventory, Inventory) -> AreCompatible when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      AreCompatible :: boolean().

are_compatible(InventoryA, InventoryB) ->
    check_one_way_compatibility(InventoryA, InventoryB)
    andalso
    check_one_way_compatibility(InventoryB, InventoryA).

-spec check_one_way_compatibility(Inventory, Inventory) -> AreCompatible when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      AreCompatible :: boolean().

check_one_way_compatibility(InventoryA, InventoryB) ->
    %% This function checks the compatibility in one way, "inventory A" is
    %% compatible with "inventory B". This is true if all feature flags
    %% enabled in "inventory B" are supported by "inventory A".
    %%
    %% They don't need to be enabled on both side: the feature flags states
    %% will be synchronized by `sync_cluster()'.
    FeatureNames = list_feature_flags_enabled_somewhere(InventoryB, true),
    lists:all(
      fun(FeatureName) ->
              #{feature_flags := #{FeatureName := FeatureFlag}} = InventoryB,
              not is_known(InventoryA, FeatureFlag)
              orelse
              is_known_and_supported(InventoryA, FeatureName)
      end, FeatureNames).

%% --------------------------------------------------------------------
%% Code to enable and sync feature flags.
%% --------------------------------------------------------------------

-spec enable_task(FeatureNames) -> Ret when
      FeatureNames :: [rabbit_feature_flags:feature_name()],
      Ret :: ok | {error, Reason},
      Reason :: term().

enable_task(FeatureNames) ->
    %% We take a snapshot of clustered nodes, including stopped nodes, at the
    %% beginning of the process and use that list at all time.
    %%
    %% If there are some missing, unreachable or stopped nodes, we abort the
    %% task and refuse to enable the feature flag(s). This is to make sure
    %% that if there is a network partition, the other side of the partition
    %% doesn't "rejoin" the cluster with some feature flags enabled behind the
    %% scene.
    %%
    %% TODO: Should we remove the requirement above and call `sync_cluster()'
    %% when a network partition is repaired?
    %%
    %% If a node tries to join the cluster during the task, it will block
    %% because it will want to synchronize its feature flags state with the
    %% cluster. For that to happen, this controller `enable' task needs to
    %% finish before the synchronization task starts.
    %%
    %% If a node stops during the task, this will trigger an RPC error at some
    %% point and the task will abort. That's ok because migration functions
    %% are supposed to be idempotent.
    AllNodes = all_nodes(),
    RunningNodes = running_nodes(),
    case RunningNodes of
        AllNodes ->
            ?LOG_DEBUG(
               "Feature flags: nodes where the feature flags will be "
               "enabled: ~p~n"
               "Feature flags: new nodes joining the cluster in between "
               "will wait and synchronize their feature flag states after.",
               [AllNodes],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            %% Likewise, we take a snapshot of the feature flags states on all
            %% running nodes right from the beginning. This is what we use
            %% during the entire task to determine if feature flags are
            %% supported, enabled somewhere, etc.
            case collect_inventory_on_nodes(AllNodes) of
                {ok, Inventory} -> enable_many(Inventory, FeatureNames);
                Error           -> Error
            end;
        _ ->
            ?LOG_ERROR(
               "Feature flags: refuse to enable feature flags while "
               "clustered nodes are missing, stopped or unreachable: ~p",
               [AllNodes -- RunningNodes]),
            {error, missing_clustered_nodes}
    end.

-spec sync_cluster_task() -> Ret when
      Ret :: ok | {error, Reason},
      Reason :: term().

sync_cluster_task() ->
    %% We assume that a feature flag can only be enabled, not disabled.
    %% Therefore this synchronization searches for feature flags enabled on
    %% some nodes but not all, and make sure they are enabled everywhere.
    %%
    %% This happens when a node joins a cluster and that node has a different
    %% set of enabled feature flags.
    %%
    %% FIXME: `enable_task()' requires that all nodes in the cluster run to
    %% enable anything. Should we require the same here? On one hand, this
    %% would make sure a feature flag isn't enabled while there is a network
    %% partition. On the other hand, this would require that all nodes are
    %% running before we can expand the cluster...
    Nodes = running_nodes(),
    ?LOG_DEBUG(
       "Feature flags: synchronizing feature flags on nodes: ~p",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

    case collect_inventory_on_nodes(Nodes) of
        {ok, Inventory} ->
            FeatureNames = list_feature_flags_enabled_somewhere(
                             Inventory, false),
            enable_many(Inventory, FeatureNames);
        Error ->
            Error
    end.

-spec refresh_after_app_load_task() -> Ret when
      Ret :: ok | {error, Reason},
      Reason :: term().

refresh_after_app_load_task() ->
    case rabbit_ff_registry_factory:initialize_registry() of
        ok    -> sync_cluster_task();
        Error -> Error
    end.

-spec enable_many(Inventory, FeatureNames) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureNames :: [rabbit_feature_flags:feature_name()],
      Ret :: ok | {error, Reason},
      Reason :: term().

enable_many(#{states_per_node := _} = Inventory, [FeatureName | Rest]) ->
    case enable_if_supported(Inventory, FeatureName) of
        ok    -> enable_many(Inventory, Rest);
        Error -> Error
    end;
enable_many(_Inventory, []) ->
    ok.

-spec enable_if_supported(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: ok | {error, Reason},
      Reason :: term().

enable_if_supported(#{states_per_node := _} = Inventory, FeatureName) ->
    case is_known_and_supported(Inventory, FeatureName) of
        true ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: supported; continuing",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            case lock_registry_and_enable(Inventory, FeatureName) of
                {ok, _Inventory} -> ok;
                Error            -> Error
            end;
        false ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: unsupported; aborting",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, unsupported}
    end.

-spec lock_registry_and_enable(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

lock_registry_and_enable(#{states_per_node := _} = Inventory, FeatureName) ->
    %% We acquire a lock before making any change to the registry. This is not
    %% used by the controller (because it is already using a globally
    %% registered name to prevent concurrent runs). But this is used in
    %% `rabbit_feature_flags:is_enabled()' to block while the state is
    %% `state_changing'.
    rabbit_ff_registry_factory:acquire_state_change_lock(),
    Ret = enable_with_registry_locked(Inventory, FeatureName),
    rabbit_ff_registry_factory:release_state_change_lock(),
    Ret.

-spec enable_with_registry_locked(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

enable_with_registry_locked(
  #{states_per_node := _} = Inventory, FeatureName) ->
    %% We verify if the feature flag needs to be enabled somewhere. This may
    %% have changed since the beginning, not because another controller
    %% enabled it (this is not possible because only one can run at a given
    %% time), but because this feature flag was already enabled as a
    %% consequence (for instance, it's a dependency of another feature flag we
    %% processed).
    Nodes = list_nodes_where_feature_flag_is_disabled(Inventory, FeatureName),
    case Nodes of
        [] ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: already enabled; skipping",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {ok, Inventory};
        _ ->
            ?LOG_NOTICE(
               "Feature flags: attempt to enable `~s`...",
               [FeatureName],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),

            case update_feature_state_and_enable(Inventory, FeatureName) of
                {ok, _Inventory} = Ok ->
                    ?LOG_NOTICE(
                       "Feature flags: `~s` enabled",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    Ok;
                Error ->
                    ?LOG_ERROR(
                       "Feature flags: failed to enable `~s`: ~p",
                       [FeatureName, Error],
                       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
                    Error
            end
    end.

-spec update_feature_state_and_enable(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

update_feature_state_and_enable(
  #{states_per_node := _} = Inventory, FeatureName) ->
    %% The feature flag is marked as `state_changing' on all running nodes who
    %% know it, including those where it's already enabled. The idea is that
    %% the feature flag is between two states on some nodes and the code
    %% running on the nodes where the feature flag enabled shouldn't assume
    %% all nodes in the cluster are in the same situation.
    Nodes = list_nodes_who_know_the_feature_flag(Inventory, FeatureName),

    %% We only consider nodes where the feature flag is disabled. The
    %% migration function is only executed on those nodes. I.e. we don't run
    %% it again where it has already been executed.
    NodesWhereDisabled = list_nodes_where_feature_flag_is_disabled(
                           Inventory, FeatureName),

    Ret1 = mark_as_enabled_on_nodes(
             Nodes, Inventory, FeatureName, state_changing),
    case Ret1 of
        %% We ignore the returned updated inventory because we don't need or
        %% even want to remember the `state_changing' state. This is only used
        %% for external queries of the registry.
        {ok, _Inventory0} ->
            case do_enable(Inventory, FeatureName, NodesWhereDisabled) of
                {ok, Inventory1} ->
                    Ret2 = mark_as_enabled_on_nodes(
                      Nodes, Inventory1, FeatureName, true),
                    case Ret2 of
                        {ok, Inventory2} ->
                            post_enable(
                              Inventory2, FeatureName, NodesWhereDisabled),
                            Ret2;
                        Error ->
                            _ = mark_as_enabled_on_nodes(
                                  Nodes, Inventory, FeatureName, false),
                            Error
                    end;
                Error ->
                    _ = mark_as_enabled_on_nodes(
                          Nodes, Inventory, FeatureName, false),
                    Error
            end;
        Error ->
            _ = mark_as_enabled_on_nodes(
                  Nodes, Inventory, FeatureName, false),
            Error
    end.

-spec do_enable(Inventory, FeatureName, Nodes) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Nodes :: [node()],
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

do_enable(#{states_per_node := _} = Inventory, FeatureName, Nodes) ->
    %% After dependencies are enabled, we need to remember the updated
    %% inventory. This is useful later to skip feature flags enabled earlier
    %% in the process. For instance because a feature flag is dependency of
    %% several other feature flags.
    case enable_dependencies(Inventory, FeatureName) of
        {ok, Inventory1} ->
            Rets = run_callback(Nodes, FeatureName, enable, #{}, infinity),
            maps:fold(
              fun
                  (_Node, ok, {ok, _} = Ret) -> Ret;
                  (_Node, Error, {ok, _})    -> Error;
                  (_Node, _, Error)          -> Error
              end, {ok, Inventory1}, Rets);
        Error ->
            Error
    end.

-spec post_enable(Inventory, FeatureName, Nodes) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Nodes :: [node()],
      Ret :: ok.

post_enable(#{states_per_node := _}, FeatureName, Nodes) ->
    case rabbit_feature_flags:uses_callbacks(FeatureName) of
        true ->
            _ = run_callback(Nodes, FeatureName, post_enable, #{}, infinity),
            ok;
        false ->
            ok
    end.

%% --------------------------------------------------------------------
%% Cluster relationship and inventory.
%% --------------------------------------------------------------------

-ifndef(TEST).
all_nodes() ->
    lists:usort([node() | mnesia:system_info(db_nodes)]).

running_nodes() ->
    lists:usort([node() | mnesia:system_info(running_db_nodes)]).
-else.
all_nodes() ->
    RemoteNodes = case rabbit_feature_flags:get_overriden_nodes() of
                      undefined -> mnesia:system_info(db_nodes);
                      Nodes     -> Nodes
                  end,
    lists:usort([node() | RemoteNodes]).

running_nodes() ->
    RemoteNodes = case rabbit_feature_flags:get_overriden_running_nodes() of
                      undefined -> mnesia:system_info(running_db_nodes);
                      Nodes     -> Nodes
                  end,
    lists:usort([node() | RemoteNodes]).
-endif.

collect_inventory_on_nodes(Nodes) ->
    collect_inventory_on_nodes(Nodes, ?TIMEOUT).

-spec collect_inventory_on_nodes(Nodes, Timeout) -> Ret when
      Nodes :: [node()],
      Timeout :: timeout(),
      Ret :: {ok, Inventory} | {error, Reason},
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      Reason :: term().

collect_inventory_on_nodes(Nodes, Timeout) ->
    ?LOG_DEBUG(
       "Feature flags: collecting inventory on nodes: ~p",
       [Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Inventory0 = #{feature_flags => #{},
                   applications_per_node => #{},
                   states_per_node => #{}},
    Rets = rpc_calls(Nodes, rabbit_ff_registry, inventory, [], Timeout),
    maps:fold(
      fun
          (Node,
           #{feature_flags := FeatureFlags1,
             applications := ScannedApps,
             states := FeatureStates},
           {ok, #{feature_flags := FeatureFlags,
                  applications_per_node := ScannedAppsPerNode,
                  states_per_node := StatesPerNode} = Inventory}) ->
            FeatureFlags2 = maps:merge(FeatureFlags, FeatureFlags1),
            ScannedAppsPerNode1 = ScannedAppsPerNode#{Node => ScannedApps},
            StatesPerNode1 = StatesPerNode#{Node => FeatureStates},
            Inventory1 = Inventory#{
                           feature_flags => FeatureFlags2,
                           applications_per_node => ScannedAppsPerNode1,
                           states_per_node => StatesPerNode1},
            {ok, Inventory1};
          (_Node, #{}, Error) ->
              Error;
          (_Node, Error, {ok, #{}}) ->
              Error;
          (_Node, _Error, Error) ->
              Error
      end, {ok, Inventory0}, Rets).

-spec list_feature_flags_enabled_somewhere(Inventory, HandleStateChanging) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      HandleStateChanging :: boolean(),
      Ret :: [FeatureName],
      FeatureName :: rabbit_feature_flags:feature_name().

list_feature_flags_enabled_somewhere(
  #{states_per_node := StatesPerNode},
  HandleStateChanging) ->
    %% We want to collect feature flags which are enabled on at least one
    %% node.
    MergedStates = maps:fold(
                     fun(_Node, FeatureStates, Acc1) ->
                             maps:fold(
                               fun
                                   (FeatureName, true, Acc2) ->
                                       Acc2#{FeatureName => true};
                                   (_FeatureName, false, Acc2) ->
                                       Acc2;
                                   (FeatureName, state_changing, Acc2)
                                     when HandleStateChanging ->
                                       Acc2#{FeatureName => true}
                               end, Acc1, FeatureStates)
                     end, #{}, StatesPerNode),
    lists:sort(maps:keys(MergedStates)).

-spec list_nodes_who_know_the_feature_flag(Inventory, FeatureName) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: [node()].

list_nodes_who_know_the_feature_flag(
  #{states_per_node := StatesPerNode},
  FeatureName) ->
    Nodes = lists:sort(
              maps:keys(
                maps:filter(
                  fun(_Node, FeatureStates) ->
                          maps:is_key(FeatureName, FeatureStates)
                  end, StatesPerNode))),
    this_node_first(Nodes).

-spec list_nodes_where_feature_flag_is_disabled(Inventory, FeatureName) ->
    Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: [node()].

list_nodes_where_feature_flag_is_disabled(
  #{states_per_node := StatesPerNode},
  FeatureName) ->
    Nodes = lists:sort(
              maps:keys(
                maps:filter(
                  fun(_Node, FeatureStates) ->
                          case FeatureStates of
                              %% The feature flag is known on this node, run
                              %% the migration function only if it is
                              %% disabled.
                              #{FeatureName := Enabled} -> not Enabled;


                              %% The feature flags is unknown on this node,
                              %% don't run the migration function.
                              _ -> false
                          end
                  end, StatesPerNode))),
    this_node_first(Nodes).

this_node_first(Nodes) ->
    ThisNode = node(),
    case lists:member(ThisNode, Nodes) of
        true  -> [ThisNode | Nodes -- [ThisNode]];
        false -> Nodes
    end.

-spec rpc_call(Node, Module, Function, Args, Timeout) -> Ret when
      Node :: node(),
      Module :: module(),
      Function :: atom(),
      Args :: [term()],
      Timeout :: timeout(),
      Ret :: term() | {error, term()}.

rpc_call(Node, Module, Function, Args, Timeout) ->
    case rpc:call(Node, Module, Function, Args, Timeout) of
        {badrpc, {'EXIT',
                  {undef,
                   [{rabbit_feature_flags, Function, Args, []}
                    | _]}}} ->
            %% If rabbit_feature_flags:Function() is undefined
            %% on the remote node, we consider it to be a 3.7.x
            %% pre-feature-flags node.
            %%
            %% Theoretically, it could be an older version (3.6.x and
            %% older). But the RabbitMQ version consistency check
            %% (rabbit_misc:version_minor_equivalent/2) called from
            %% rabbit_mnesia:check_rabbit_consistency/2 already blocked
            %% this situation from happening before we reach this point.
            ?LOG_DEBUG(
               "Feature flags: ~s:~s~p unavailable on node `~s`: "
               "assuming it is a RabbitMQ 3.7.x pre-feature-flags node",
               [?MODULE, Function, Args, Node],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, pre_feature_flags_rabbitmq};
        {badrpc, Reason} = Error ->
            ?LOG_ERROR(
               "Feature flags: error while running:~n"
               "Feature flags:   ~s:~s~p~n"
               "Feature flags: on node `~s`:~n"
               "Feature flags:   ~p",
               [?MODULE, Function, Args, Node, Reason],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            {error, Error};
        Ret ->
            Ret
    end.

-spec rpc_calls(Nodes, Module, Function, Args, Timeout) -> Rets when
      Nodes :: [node()],
      Module :: module(),
      Function :: atom(),
      Args :: [term()],
      Timeout :: timeout(),
      Rets :: #{node() => term()}.

rpc_calls(Nodes, Module, Function, Args, Timeout) when is_list(Nodes) ->
    Parent = self(),
    CallRef = {Module, Function, Args},
    Runners = [{Node,
                spawn_monitor(
                  fun() ->
                          Ret = rpc_call(
                                  Node, Module, Function, Args, Timeout),
                          Parent ! {internal_rpc_call, Node, CallRef, Ret}
                  end)}
               || Node <- Nodes],
    Rets = [receive
                {internal_rpc_call, Node, CallRef, Ret} ->
                    %% After we got the return value for that node, we still
                    %% need to consume the `DOWN' message emitted once the
                    %% spawn process exited.
                    receive
                        {'DOWN', MRef, process, Pid, Reason} ->
                            ?assertEqual(normal, Reason)
                    end,
                    {Node, Ret};
                {'DOWN', MRef, process, Pid, Reason} ->
                    {Node, {error, {'DOWN', Reason}}}
            end
            || {Node, {Pid, MRef}} <- Runners],
    maps:from_list(Rets).

%% --------------------------------------------------------------------
%% Feature flag support queries.
%% --------------------------------------------------------------------

-spec is_known(Inventory, FeatureFlag) -> IsKnown when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureFlag :: rabbit_feature_flags:feature_props_extended(),
      IsKnown :: boolean().

is_known(
  #{applications_per_node := ScannedAppsPerNode},
  #{provided_by := App} = _FeatureFlag) ->
    maps:fold(
      fun
          (_Node, ScannedApps, false) -> lists:member(App, ScannedApps);
          (_Node, _ScannedApps, true) -> true
      end, false, ScannedAppsPerNode).

-spec is_known_and_supported(Inventory, FeatureName) ->
    IsKnownAndSupported when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      IsKnownAndSupported :: boolean().

is_known_and_supported(
  #{feature_flags := FeatureFlags,
    applications_per_node := ScannedAppsPerNode,
    states_per_node := StatesPerNode},
  FeatureName)
  when is_map_key(FeatureName, FeatureFlags) ->
    %% A feature flag is considered supported by a node if:
    %%   - the node knows the feature flag, or
    %%   - the node does not have the application providing it.
    %%
    %% Therefore, we first need to look up the application providing this
    %% feature flag.
    #{FeatureName := #{provided_by := App}} = FeatureFlags,

    maps:fold(
      fun
          (Node, FeatureStates, true) ->
              case FeatureStates of
                  #{FeatureName := _} ->
                      %% The node knows about the feature flag.
                      true;
                  _ ->
                      %% The node doesn't know about the feature flag, so does
                      %% it have the application providing it loaded?
                      #{Node := ScannedApps} = ScannedAppsPerNode,
                      not lists:member(App, ScannedApps)
              end;
          (_Node, _FeatureStates, false) ->
              false
      end, true, StatesPerNode);
is_known_and_supported(_Inventory, _FeatureName) ->
    %% None of the nodes know about this feature flag at all.
    false.

%% --------------------------------------------------------------------
%% Feature flag state changes.
%% --------------------------------------------------------------------

-spec mark_as_enabled_on_nodes(Nodes, Inventory, FeatureName, IsEnabled) ->
    Ret when
      Nodes :: [node()],
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      IsEnabled :: rabbit_feature_flags:feature_state(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

mark_as_enabled_on_nodes(
  Nodes,
  #{states_per_node := StatesPerNode} = Inventory,
  FeatureName, IsEnabled) ->
    ?LOG_DEBUG(
       "Feature flags: `~s`: mark as enabled=~p on nodes ~p",
       [FeatureName, IsEnabled, Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets = rpc_calls(
             Nodes, rabbit_feature_flags, mark_as_enabled_locally,
             [FeatureName, IsEnabled], ?TIMEOUT),
    Ret = maps:fold(
            fun
                (Node, ok, {ok, StatesPerNode1}) ->
                    FeatureStates1 = maps:get(Node, StatesPerNode1),
                    FeatureStates2 = FeatureStates1#{
                                       FeatureName => IsEnabled},
                    StatesPerNode2 = StatesPerNode1#{
                                       Node => FeatureStates2},
                    {ok, StatesPerNode2};
                (_Node, ok, Error) ->
                    Error;
                (_Node, Error, {ok, _StatesPerNode1}) ->
                    Error;
                (_Node, _Error, Error) ->
                    Error
            end, {ok, StatesPerNode}, Rets),
    case Ret of
        {ok, StatesPerNode3} ->
            Inventory1 = Inventory#{states_per_node => StatesPerNode3},
            {ok, Inventory1};
        Error ->
            Error
    end.

%% --------------------------------------------------------------------
%% Feature flag dependencies handling.
%% --------------------------------------------------------------------

-spec enable_dependencies(Inventory, FeatureName) -> Ret when
      Inventory :: rabbit_feature_flags:cluster_inventory(),
      FeatureName :: rabbit_feature_flags:feature_name(),
      Ret :: {ok, Inventory} | {error, Reason},
      Reason :: term().

enable_dependencies(
  #{feature_flags := FeatureFlags} = Inventory, FeatureName) ->
    #{FeatureName := FeatureProps} = FeatureFlags,
    DependsOn = maps:get(depends_on, FeatureProps, []),
    case DependsOn of
        [] ->
            {ok, Inventory};
        _ ->
            ?LOG_DEBUG(
               "Feature flags: `~s`: enable dependencies: ~p",
               [FeatureName, DependsOn],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            enable_dependencies1(Inventory, DependsOn)
    end.

enable_dependencies1(
  #{states_per_node := _} = Inventory, [FeatureName | Rest]) ->
    case enable_with_registry_locked(Inventory, FeatureName) of
        {ok, Inventory1} -> enable_dependencies1(Inventory1, Rest);
        Error            -> Error
    end;
enable_dependencies1(
  #{states_per_node := _} = Inventory, []) ->
    {ok, Inventory}.

%% --------------------------------------------------------------------
%% Migration function.
%% --------------------------------------------------------------------

-spec run_callback(Nodes, FeatureName, Command, Extra, Timeout) ->
    Rets when
      Nodes :: [node()],
      FeatureName :: rabbit_feature_flags:feature_name(),
      Command :: rabbit_feature_flags:callback_name(),
      Extra :: rabbit_feature_flags:callbacks_args(),
      Timeout :: timeout(),
      Rets :: #{node() => term()}.

run_callback(Nodes, FeatureName, Command, Extra, Timeout) ->
    FeatureProps = rabbit_ff_registry:get(FeatureName),
    Callbacks = maps:get(callbacks, FeatureProps, #{}),
    case Callbacks of
        #{Command := {CallbackMod, CallbackFun}}
          when is_atom(CallbackMod) andalso is_atom(CallbackFun) ->
            %% The migration fun API v2 is of the form:
            %%   CallbackMod:CallbackFun(...)
            %%
            %% Also, the function is executed on all nodes in parallel.
            Args = Extra#{feature_name => FeatureName,
                          feature_props => FeatureProps,
                          command => Command,
                          nodes => Nodes},
            do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout);
        #{Command := Invalid} ->
            ?LOG_ERROR(
               "Feature flags: `~s`: invalid callback for `~s`: ~p",
               [FeatureName, Command, Invalid],
               #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
            #{node() =>
              {error, {invalid_callback, FeatureName, Command, Invalid}}};
        _ ->
            %% The migration fun API v1 is of the form:
            %%   MigrationMod:MigrationFun(
            %%     FeatureName, FeatureProps, Command).
            %%
            %% Also, the function is executed once on the calling node only.
            %%
            %% Only `enable' is supported by the v1 migration function.
            Ret = case Command of
                      enable ->
                          Ret0 = rabbit_feature_flags:run_migration_fun(
                                   FeatureName, FeatureProps, Command),
                          case Ret0 of
                              {error, no_migration_fun} -> ok;
                              _                         -> Ret0
                          end;
                      _ ->
                          ok
                  end,
            #{node() => Ret}
    end.

-spec do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout) ->
    Rets when
      Nodes :: [node()],
      CallbackMod :: module(),
      CallbackFun :: atom(),
      Args :: rabbit_feature_flags:callbacks_args(),
      Timeout :: timeout(),
      Rets :: #{node() => rabbit_feature_flags:callbacks_rets()}.

do_run_callback(Nodes, CallbackMod, CallbackFun, Args, Timeout) ->
    #{feature_name := FeatureName,
      command := Command} = Args,
    ?LOG_DEBUG(
       "Feature flags: `~s`: run callback ~s:~s (~s callback)~n"
       "Feature flags:   with args = ~p~n"
       "Feature flags:   on nodes ~p",
       [FeatureName, CallbackMod, CallbackFun, Command, Args, Nodes],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets = rpc_calls(Nodes, CallbackMod, CallbackFun, [Args], Timeout),
    ?LOG_DEBUG(
       "Feature flags: `~s`: callback ~s:~s (~s callback) returned:~n"
       "Feature flags:   ~p",
       [FeatureName, CallbackMod, CallbackFun, Command, Rets],
       #{domain => ?RMQLOG_DOMAIN_FEAT_FLAGS}),
    Rets.
