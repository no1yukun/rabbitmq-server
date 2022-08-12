%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2020-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(commands_SUITE).

-compile(nowarn_export_all).
-compile([export_all]).

% -include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").
-include_lib("rabbitmq_stream_common/include/rabbit_stream.hrl").

-define(WAIT, 5000).
-define(COMMAND_LIST_CONNECTIONS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConnectionsCommand').
-define(COMMAND_LIST_CONSUMERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumersCommand').
-define(COMMAND_LIST_PUBLISHERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamPublishersCommand').
-define(COMMAND_ADD_SUPER_STREAM,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.AddSuperStreamCommand').
-define(COMMAND_DELETE_SUPER_STREAM,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.DeleteSuperStreamCommand').
-define(COMMAND_LIST_CONSUMER_GROUPS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamConsumerGroupsCommand').
-define(COMMAND_LIST_GROUP_CONSUMERS,
        'Elixir.RabbitMQ.CLI.Ctl.Commands.ListStreamGroupConsumersCommand').

all() ->
    [{group, list_connections},
     {group, list_consumers},
     {group, list_publishers},
     {group, list_consumer_groups},
     {group, list_group_consumers},
     {group, super_streams}].

groups() ->
    [{list_connections, [],
      [list_connections_merge_defaults, list_connections_run,
       list_tls_connections_run]},
     {list_consumers, [],
      [list_consumers_merge_defaults, list_consumers_run]},
     {list_publishers, [],
      [list_publishers_merge_defaults, list_publishers_run]},
     {list_consumer_groups, [],
      [list_consumer_groups_merge_defaults, list_consumer_groups_run]},
     {list_group_consumers, [],
      [list_group_consumers_validate, list_group_consumers_merge_defaults,
       list_group_consumers_run]},
     {super_streams, [],
      [add_super_stream_merge_defaults,
       add_super_stream_validate,
       delete_super_stream_merge_defaults,
       delete_super_stream_validate,
       add_delete_super_stream_run]}].

init_per_suite(Config) ->
    case rabbit_ct_helpers:is_mixed_versions() of
        true ->
            {skip,
             "mixed version clusters are not supported for "
             "this suite"};
        _ ->
            Config1 =
                rabbit_ct_helpers:set_config(Config,
                                             [{rmq_nodename_suffix, ?MODULE}]),
            Config2 =
                rabbit_ct_helpers:set_config(Config1,
                                             {rabbitmq_ct_tls_verify,
                                              verify_none}),
            rabbit_ct_helpers:log_environment(),
            rabbit_ct_helpers:run_setup_steps(Config2,
                                              rabbit_ct_broker_helpers:setup_steps())
    end.

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config,
                                         rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) ->
    Config.

end_per_group(_, Config) ->
    Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

list_connections_merge_defaults(_Config) ->
    {[<<"conn_name">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONNECTIONS:merge_defaults([<<"other_key">>],
                                                 #{verbose => false}).

list_connections_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false},

    %% No connections
    [] = to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),

    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    [[{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),
    [[{ssl, false}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"ssl">>], Opts)),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),

    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    Port =
        rabbit_ct_broker_helpers:get_node_config(Config, 0, tcp_port_amqp),
    start_amqp_connection(network, Node, Port),

    %% There are still just two connections
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    start_amqp_connection(direct, Node, Port),

    %% Still two stream connections, one direct AMQP 0-9-1 connection
    [[{conn_name, _}], [{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),

    %% Verbose returns all keys
    Infos =
        lists:map(fun(El) -> atom_to_binary(El, utf8) end, ?INFO_ITEMS),
    AllKeys = to_list(?COMMAND_LIST_CONNECTIONS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),

    %% There are two connections
    [First, _Second] = AllKeys,

    %% Keys are INFO_ITEMS
    ?assertEqual(length(?INFO_ITEMS), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- ?INFO_ITEMS),
    ?assertEqual([], ?INFO_ITEMS -- Keys),

    rabbit_stream_SUITE:test_close(gen_tcp, S1, C1),
    rabbit_stream_SUITE:test_close(gen_tcp, S2, C2),
    ok.

list_tls_connections_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false},

    %% No connections
    [] = to_list(?COMMAND_LIST_CONNECTIONS:run([], Opts)),

    StreamTlsPort = rabbit_stream_SUITE:get_stream_port_tls(Config),
    application:ensure_all_started(ssl),

    {S1, C1} = start_stream_tls_connection(StreamTlsPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    [[{conn_name, _}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>], Opts)),
    [[{ssl, true}]] =
        to_list(?COMMAND_LIST_CONNECTIONS:run([<<"ssl">>], Opts)),

    rabbit_stream_SUITE:test_close(ssl, S1, C1),
    ok.

list_consumers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?CONSUMER_INFO_ITEMS],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_CONSUMERS:merge_defaults([<<"other_key">>],
                                               #{verbose => false}).

list_consumers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no consumers
    [] = to_list(?COMMAND_LIST_CONSUMERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    Stream = <<"list_consumers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    SubId = 42,
    C1_2 = subscribe(S1, SubId, Stream, C1_1),

    ?awaitMatch(1, consumer_count(Config), ?WAIT),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),
    C2_1 = subscribe(S2, SubId, Stream, C2),

    ?awaitMatch(2, consumer_count(Config), ?WAIT),

    %% Verbose returns all keys
    InfoItems = ?CONSUMER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_CONSUMERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_CONSUMERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two consumers
    [First, _Second] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_1),
    ?awaitMatch(0, consumer_count(Config), ?WAIT),
    ok.

list_publishers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?PUBLISHER_INFO_ITEMS],
    {DefaultItems, #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => true}),

    {[<<"other_key">>], #{verbose := false}} =
        ?COMMAND_LIST_PUBLISHERS:merge_defaults([<<"other_key">>],
                                                #{verbose => false}).

list_publishers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>},

    %% No connections, no publishers
    [] = to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts)),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S1, C1} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    Stream = <<"list_publishers_run">>,
    C1_1 = create_stream(S1, Stream, C1),
    PubId = 42,
    C1_2 = declare_publisher(S1, PubId, Stream, C1_1),

    ?awaitMatch(1, publisher_count(Config), ?WAIT),

    {S2, C2} = start_stream_connection(StreamPort),
    ?awaitMatch(2, connection_count(Config), ?WAIT),
    C2_1 = declare_publisher(S2, PubId, Stream, C2),

    ?awaitMatch(2, publisher_count(Config), ?WAIT),

    %% Verbose returns all keys
    InfoItems = ?PUBLISHER_INFO_ITEMS,
    Infos = lists:map(fun(El) -> atom_to_binary(El, utf8) end, InfoItems),
    AllKeys = to_list(?COMMAND_LIST_PUBLISHERS:run(Infos, Opts)),
    Verbose =
        to_list(?COMMAND_LIST_PUBLISHERS:run([], Opts#{verbose => true})),
    ?assertEqual(AllKeys, Verbose),
    %% There are two publishers
    [First, _Second] = AllKeys,

    %% Keys are info items
    ?assertEqual(length(InfoItems), length(First)),

    {Keys, _} = lists:unzip(First),

    ?assertEqual([], Keys -- InfoItems),
    ?assertEqual([], InfoItems -- Keys),

    C1_3 = delete_stream(S1, Stream, C1_2),
    % metadata_update_stream_deleted(S1, Stream),
    C2_2 = metadata_update_stream_deleted(S2, Stream, C2_1),
    close(S1, C1_3),
    close(S2, C2_2),
    ?awaitMatch(0, publisher_count(Config), ?WAIT),
    ok.

list_consumer_groups_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?CONSUMER_GROUP_INFO_ITEMS],
    {DefaultItems, #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([<<"other_key">>],
                                                     #{verbose => true}),

    {[<<"other_key">>], #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_CONSUMER_GROUPS:merge_defaults([<<"other_key">>],
                                                     #{verbose => false}).

list_consumer_groups_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>,
          verbose => true},

    %% No connections, no consumers
    {ok, []} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    ConsumerReference = <<"foo">>,
    SubProperties =
        #{<<"single-active-consumer">> => <<"true">>,
          <<"name">> => ConsumerReference},

    Stream1 = <<"list_consumer_groups_run_1">>,
    create_stream(S, Stream1, C),
    subscribe(S, 0, Stream1, SubProperties, C),
    handle_consumer_update(S, C, 0),
    subscribe(S, 1, Stream1, SubProperties, C),
    subscribe(S, 2, Stream1, SubProperties, C),

    ?awaitMatch(3, consumer_count(Config), ?WAIT),

    {ok, [CG1]} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    assertConsumerGroup(Stream1, ConsumerReference, -1, 3, CG1),

    Stream2 = <<"list_consumer_groups_run_2">>,
    create_stream(S, Stream2, C),
    subscribe(S, 3, Stream2, SubProperties, C),
    handle_consumer_update(S, C, 3),
    subscribe(S, 4, Stream2, SubProperties, C),
    subscribe(S, 5, Stream2, SubProperties, C),

    ?awaitMatch(3 + 3, consumer_count(Config), ?WAIT),

    {ok, [CG1, CG2]} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    assertConsumerGroup(Stream1, ConsumerReference, -1, 3, CG1),
    assertConsumerGroup(Stream2, ConsumerReference, -1, 3, CG2),

    delete_stream(S, Stream1, C),
    delete_stream(S, Stream2, C),

    close(S, C),
    {ok, []} = ?COMMAND_LIST_CONSUMER_GROUPS:run([], Opts),
    ok.

list_group_consumers_validate(_) ->
    ValidOpts =
        #{vhost => <<"/">>,
          stream => <<"s1">>,
          reference => <<"foo">>},
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([], #{})),
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([],
                                                        #{vhost =>
                                                              <<"test">>})),
    ?assertMatch({validation_failure, {bad_info_key, [foo]}},
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([<<"foo">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([<<"subscription_id">>],
                                                        ValidOpts)),
    ?assertMatch(ok,
                 ?COMMAND_LIST_GROUP_CONSUMERS:validate([], ValidOpts)).

list_group_consumers_merge_defaults(_Config) ->
    DefaultItems =
        [rabbit_data_coercion:to_binary(Item)
         || Item <- ?GROUP_CONSUMER_INFO_ITEMS],
    {DefaultItems, #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([], #{}),

    {[<<"other_key">>], #{verbose := true, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([<<"other_key">>],
                                                     #{verbose => true}),

    {[<<"other_key">>], #{verbose := false, vhost := <<"/">>}} =
        ?COMMAND_LIST_GROUP_CONSUMERS:merge_defaults([<<"other_key">>],
                                                     #{verbose => false}).

list_group_consumers_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>,
          verbose => false},
    Args = [<<"subscription_id">>, <<"state">>],

    Stream1 = <<"list_group_consumers_run_1">>,
    ConsumerReference = <<"foo">>,
    OptsGroup1 =
        maps:merge(#{stream => Stream1, reference => ConsumerReference},
                   Opts),

    %% the group does not exist yet
    {error, not_found} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup1),

    StreamPort = rabbit_stream_SUITE:get_stream_port(Config),
    {S, C} = start_stream_connection(StreamPort),
    ?awaitMatch(1, connection_count(Config), ?WAIT),

    SubProperties =
        #{<<"single-active-consumer">> => <<"true">>,
          <<"name">> => ConsumerReference},

    create_stream(S, Stream1, C),
    subscribe(S, 0, Stream1, SubProperties, C),
    handle_consumer_update(S, C, 0),
    subscribe(S, 1, Stream1, SubProperties, C),
    subscribe(S, 2, Stream1, SubProperties, C),

    ?awaitMatch(3, consumer_count(Config), ?WAIT),

    {ok, Consumers1} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup1),
    ?assertEqual([[{subscription_id, 0}, {state, active}],
                  [{subscription_id, 1}, {state, inactive}],
                  [{subscription_id, 2}, {state, inactive}]],
                 Consumers1),

    Stream2 = <<"list_group_consumers_run_2">>,
    OptsGroup2 =
        maps:merge(#{stream => Stream2, reference => ConsumerReference},
                   Opts),

    create_stream(S, Stream2, C),
    subscribe(S, 3, Stream2, SubProperties, C),
    handle_consumer_update(S, C, 3),
    subscribe(S, 4, Stream2, SubProperties, C),
    subscribe(S, 5, Stream2, SubProperties, C),

    ?awaitMatch(3 + 3, consumer_count(Config), ?WAIT),

    {ok, Consumers2} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup2),
    ?assertEqual([[{subscription_id, 3}, {state, active}],
                  [{subscription_id, 4}, {state, inactive}],
                  [{subscription_id, 5}, {state, inactive}]],
                 Consumers2),

    delete_stream(S, Stream1, C),
    delete_stream(S, Stream2, C),

    {error, not_found} =
        ?COMMAND_LIST_GROUP_CONSUMERS:run(Args, OptsGroup2),

    close(S, C),
    ok.

handle_consumer_update(S, C0, SubId) ->
    {{request, CorrId, {consumer_update, SubId, true}}, C1} =
        rabbit_stream_SUITE:receive_commands(gen_tcp, S, C0),
    ConsumerUpdateCmd =
        {response, CorrId, {consumer_update, ?RESPONSE_CODE_OK, next}},
    ConsumerUpdateFrame = rabbit_stream_core:frame(ConsumerUpdateCmd),
    ok = gen_tcp:send(S, ConsumerUpdateFrame),
    C1.

assertConsumerGroup(S, R, PI, Cs, Record) ->
    ?assertEqual(S, proplists:get_value(stream, Record)),
    ?assertEqual(R, proplists:get_value(reference, Record)),
    ?assertEqual(PI, proplists:get_value(partition_index, Record)),
    ?assertEqual(Cs, proplists:get_value(consumers, Record)),
    ok.

add_super_stream_merge_defaults(_Config) ->
    ?assertMatch({[<<"super-stream">>],
                  #{partitions := 3, vhost := <<"/">>}},
                 ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                          #{})),

    ?assertMatch({[<<"super-stream">>],
                  #{partitions := 5, vhost := <<"/">>}},
                 ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                          #{partitions => 5})),

    DefaultWithRoutingKeys =
        ?COMMAND_ADD_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                 #{routing_keys =>
                                                       <<"amer,emea,apac">>}),
    ?assertMatch({[<<"super-stream">>],
                  #{routing_keys := <<"amer,emea,apac">>, vhost := <<"/">>}},
                 DefaultWithRoutingKeys),

    {_, Opts} = DefaultWithRoutingKeys,
    ?assertEqual(false, maps:is_key(partitions, Opts)).

add_super_stream_validate(_Config) ->
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_ADD_SUPER_STREAM:validate([], #{})),
    ?assertMatch({validation_failure, too_many_args},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>, <<"b">>], #{})),
    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 1,
                                                      routing_keys =>
                                                          <<"a,b,c">>})),
    ?assertMatch({validation_failure, _},
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 0})),
    ?assertEqual(ok,
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{partitions => 5})),
    ?assertEqual(ok,
                 ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>],
                                                    #{routing_keys =>
                                                          <<"a,b,c">>})),

    [case Expected of
         ok ->
             ?assertEqual(ok,
                          ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], Opts));
         error ->
             ?assertMatch({validation_failure, _},
                          ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], Opts))
     end
     || {Opts, Expected}
            <- [{#{max_length_bytes => 1000}, ok},
                {#{max_length_bytes => <<"1000">>}, ok},
                {#{max_length_bytes => <<"100gb">>}, ok},
                {#{max_length_bytes => <<"50mb">>}, ok},
                {#{max_length_bytes => <<"50bm">>}, error},
                {#{max_age => <<"PT10M">>}, ok},
                {#{max_age => <<"P5DT8H">>}, ok},
                {#{max_age => <<"foo">>}, error},
                {#{stream_max_segment_size_bytes => 1000}, ok},
                {#{stream_max_segment_size_bytes => <<"1000">>}, ok},
                {#{stream_max_segment_size_bytes => <<"100gb">>}, ok},
                {#{stream_max_segment_size_bytes => <<"50mb">>}, ok},
                {#{stream_max_segment_size_bytes => <<"50bm">>}, error},
                {#{leader_locator => <<"client-local">>}, ok},
                {#{leader_locator => <<"least-leaders">>}, ok},
                {#{leader_locator => <<"random">>}, ok},
                {#{leader_locator => <<"foo">>}, error},
                {#{initial_cluster_size => <<"1">>}, ok},
                {#{initial_cluster_size => <<"2">>}, ok},
                {#{initial_cluster_size => <<"3">>}, ok},
                {#{initial_cluster_size => <<"0">>}, error},
                {#{initial_cluster_size => <<"-1">>}, error},
                {#{initial_cluster_size => <<"foo">>}, error}]],
    ok.

delete_super_stream_merge_defaults(_Config) ->
    ?assertMatch({[<<"super-stream">>], #{vhost := <<"/">>}},
                 ?COMMAND_DELETE_SUPER_STREAM:merge_defaults([<<"super-stream">>],
                                                             #{})),
    ok.

delete_super_stream_validate(_Config) ->
    ?assertMatch({validation_failure, not_enough_args},
                 ?COMMAND_DELETE_SUPER_STREAM:validate([], #{})),
    ?assertMatch({validation_failure, too_many_args},
                 ?COMMAND_DELETE_SUPER_STREAM:validate([<<"a">>, <<"b">>],
                                                       #{})),
    ?assertEqual(ok, ?COMMAND_ADD_SUPER_STREAM:validate([<<"a">>], #{})),
    ok.

add_delete_super_stream_run(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          vhost => <<"/">>},

    % with number of partitions
    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(#{partitions => 3},
                                                          Opts))),
    ?assertEqual({ok,
                  [<<"invoices-0">>, <<"invoices-1">>, <<"invoices-2">>]},
                 partitions(Config, <<"invoices">>)),
    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM:run([<<"invoices">>], Opts)),
    ?assertEqual({error, stream_not_found},
                 partitions(Config, <<"invoices">>)),

    % with routing keys
    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(#{routing_keys =>
                                                                <<" amer,emea , apac">>},
                                                          Opts))),
    ?assertEqual({ok,
                  [<<"invoices-amer">>, <<"invoices-emea">>,
                   <<"invoices-apac">>]},
                 partitions(Config, <<"invoices">>)),
    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM:run([<<"invoices">>], Opts)),
    ?assertEqual({error, stream_not_found},
                 partitions(Config, <<"invoices">>)),

    % with arguments
    ExtraOptions =
        #{partitions => 3,
          max_length_bytes => <<"50mb">>,
          max_age => <<"PT10M">>,
          stream_max_segment_size_bytes => <<"1mb">>,
          leader_locator => <<"random">>,
          initial_cluster_size => <<"1">>},

    ?assertMatch({ok, _},
                 ?COMMAND_ADD_SUPER_STREAM:run([<<"invoices">>],
                                               maps:merge(ExtraOptions, Opts))),

    {ok, Q} = queue_lookup(Config, <<"invoices-0">>),
    Args = amqqueue:get_arguments(Q),
    ?assertMatch({_, <<"random">>},
                 rabbit_misc:table_lookup(Args, <<"x-queue-leader-locator">>)),
    ?assertMatch({_, 1},
                 rabbit_misc:table_lookup(Args, <<"x-initial-cluster-size">>)),
    ?assertMatch({_, 1000000},
                 rabbit_misc:table_lookup(Args,
                                          <<"x-stream-max-segment-size-bytes">>)),
    ?assertMatch({_, <<"600s">>},
                 rabbit_misc:table_lookup(Args, <<"x-max-age">>)),
    ?assertMatch({_, 50000000},
                 rabbit_misc:table_lookup(Args, <<"x-max-length-bytes">>)),
    ?assertMatch({_, <<"stream">>},
                 rabbit_misc:table_lookup(Args, <<"x-queue-type">>)),

    ?assertMatch({ok, _},
                 ?COMMAND_DELETE_SUPER_STREAM:run([<<"invoices">>], Opts)),

    ok.

partitions(Config, Name) ->
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_stream_manager,
                                 partitions,
                                 [<<"/">>, Name]).

create_stream(S, Stream, C0) ->
    rabbit_stream_SUITE:test_create_stream(gen_tcp, S, Stream, C0).

subscribe(S, SubId, Stream, SubProperties, C) ->
    rabbit_stream_SUITE:test_subscribe(gen_tcp,
                                       S,
                                       SubId,
                                       Stream,
                                       SubProperties,
                                       C).

subscribe(S, SubId, Stream, C) ->
    rabbit_stream_SUITE:test_subscribe(gen_tcp, S, SubId, Stream, C).

declare_publisher(S, PubId, Stream, C) ->
    rabbit_stream_SUITE:test_declare_publisher(gen_tcp,
                                               S,
                                               PubId,
                                               Stream,
                                               C).

delete_stream(S, Stream, C) ->
    rabbit_stream_SUITE:test_delete_stream(gen_tcp, S, Stream, C).

metadata_update_stream_deleted(S, Stream, C) ->
    rabbit_stream_SUITE:test_metadata_update_stream_deleted(gen_tcp,
                                                            S,
                                                            Stream,
                                                            C).

close(S, C) ->
    rabbit_stream_SUITE:test_close(gen_tcp, S, C).

options(Config) ->
    Node = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    Opts =
        #{node => Node,
          timeout => 10000,
          verbose => false,
          vhost => <<"/">>}, %% just for list_consumers and list_publishers
    Opts.

flatten_command_result([], []) ->
    [];
flatten_command_result([], Acc) ->
    lists:reverse(Acc);
flatten_command_result([[{_K, _V} | _RecordRest] = Record | Rest],
                       Acc) ->
    flatten_command_result(Rest, [Record | Acc]);
flatten_command_result([H | T], Acc) ->
    Acc1 = flatten_command_result(H, Acc),
    flatten_command_result(T, Acc1).

to_list(CommandRun) ->
    Lists = 'Elixir.Enum':to_list(CommandRun),
    %% we can get results from different connections, so we flatten out
    flatten_command_result(Lists, []).

command_result_count(CommandRun) ->
    length(to_list(CommandRun)).

connection_count(Config) ->
    command_result_count(?COMMAND_LIST_CONNECTIONS:run([<<"conn_name">>],
                                                       options(Config))).

consumer_count(Config) ->
    command_result_count(?COMMAND_LIST_CONSUMERS:run([<<"stream">>],
                                                     options(Config))).

publisher_count(Config) ->
    command_result_count(?COMMAND_LIST_PUBLISHERS:run([<<"stream">>],
                                                      options(Config))).

start_stream_connection(Port) ->
    start_stream_connection(gen_tcp, Port).

start_stream_tls_connection(Port) ->
    start_stream_connection(ssl, Port).

start_stream_connection(Transport, Port) ->
    {ok, S} =
        Transport:connect("localhost", Port,
                          [{active, false}, {mode, binary}]),
    C0 = rabbit_stream_core:init(0),
    C1 = rabbit_stream_SUITE:test_peer_properties(Transport, S, C0),
    C = rabbit_stream_SUITE:test_authenticate(Transport, S, C1),
    {S, C}.

start_amqp_connection(Type, Node, Port) ->
    Params = amqp_params(Type, Node, Port),
    {ok, _Connection} = amqp_connection:start(Params).

amqp_params(network, _, Port) ->
    #amqp_params_network{port = Port};
amqp_params(direct, Node, _) ->
    #amqp_params_direct{node = Node}.

queue_lookup(Config, Q) ->
    QueueName = rabbit_misc:r(<<"/">>, queue, Q),
    rabbit_ct_broker_helpers:rpc(Config,
                                 0,
                                 rabbit_amqqueue,
                                 lookup,
                                 [QueueName]).
