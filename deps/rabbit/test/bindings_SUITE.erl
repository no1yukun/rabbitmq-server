%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2018-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(bindings_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile([nowarn_export_all, export_all]).
-compile(export_all).

suite() ->
    [{timetrap, 5 * 60000}].

all() ->
    [
     {group, mnesia_store},
     {group, khepri_store}
    ].

groups() ->
    [
     {mnesia_store, [], all_tests()},
     {khepri_store, [], all_tests()}
    ].

%% TODO run in mnesia and khepri
%% TODO run migration policies

all_tests() ->
    [
     bind_and_unbind,
     bind_and_delete,
     list_bindings,
     list_for_source,
     list_for_destination,
     list_for_source_and_destination,
     list_explicit,
     info_all,
     list_with_multiple_vhosts,
     list_with_multiple_arguments
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    rabbit_ct_helpers:run_setup_steps(Config, []).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(Config).

init_per_group(mnesia_store = Group, Config) ->
    init_per_group_common(Group, Config);
init_per_group(khepri_store = Group, Config0) ->
    Config = init_per_group_common(Group, Config0),
    case rabbit_ct_broker_helpers:enable_feature_flag(Config, raft_based_metadata_store_phase1) of
        ok ->
            Config;
        Skip ->
            end_per_group(Group, Config),
            Skip
    end.

init_per_group_common(Group, Config) ->
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodes_count, 1},
                                            {rmq_nodename_suffix, Group},
                                            {tcp_ports_base}]),
    rabbit_ct_helpers:run_steps(Config1, rabbit_ct_broker_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_steps(Config,
                                rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:testcase_started(Config, Testcase),
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Q = rabbit_data_coercion:to_binary(Testcase),
    Config2 = rabbit_ct_helpers:set_config(Config1,
                                           [{queue_name, Q},
                                            {alt_queue_name, <<Q/binary, "_alt">>}
                                           ]),
    rabbit_ct_helpers:run_steps(Config2, rabbit_ct_client_helpers:setup_steps()).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, ?MODULE, delete_queues, []),
    Config1 = rabbit_ct_helpers:run_steps(
                Config,
                rabbit_ct_client_helpers:teardown_steps()),
    rabbit_ct_helpers:testcase_finished(Config1, Testcase).

%% TODO run for both mnesia and khepri!

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------
bind_and_unbind(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),
    
    #'queue.unbind_ok'{} = amqp_channel:call(Ch, #'queue.unbind'{exchange = <<"amq.direct">>,
                                                                 queue = Q,
                                                                 routing_key = Q}),
    
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

bind_and_delete(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to other exchange
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),
    
    ?assertMatch(#'queue.delete_ok'{},
                 amqp_channel:call(Ch, #'queue.delete'{queue = Q})),
    
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    ok.

list_bindings(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),

    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind to all other exchanges
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.fanout">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.match">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.rabbitmq.trace">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                   QResource, Q, []),
    FanoutBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                                   QResource, Q, []),
    HeadersBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                    QResource, Q, []),
    MatchBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.match">>),
                                  QResource, Q, []),
    TraceBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.rabbitmq.trace">>),
                                  QResource, Q, []),
    TopicBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
                                  QResource, Q, []),
    Bindings = lists:sort([DefaultBinding, DirectBinding, FanoutBinding, HeadersBinding,
                           MatchBinding, TraceBinding, TopicBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ok.

list_for_source(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    DirectABinding = binding_record(DirectExchange, QAltResource, QAlt, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    TopicABinding = binding_record(TopicExchange, QAltResource, QAlt, []),
    DirectBindings = lists:sort([DirectBinding, DirectABinding]),
    TopicBindings = lists:sort([TopicBinding, TopicABinding]),
    
    ?assertEqual(
       DirectBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange]))),
    ?assertEqual(
       TopicBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange]))).    

list_with_multiple_vhosts(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    VHost1 = <<"vhost1">>,
    VHost2 = <<"vhost2">>,
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost1),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost1),
    ok = rabbit_ct_broker_helpers:add_vhost(Config, VHost2),
    ok = rabbit_ct_broker_helpers:set_full_permissions(Config, <<"guest">>, VHost2),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Conn1 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost1),
    {ok, Ch1} = amqp_connection:open_channel(Conn1),
    Conn2 = rabbit_ct_client_helpers:open_unmanaged_connection(Config, 0, VHost2),
    {ok, Ch2} = amqp_connection:open_channel(Conn2),

    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch1, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch1, QAlt, [])),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch2, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch2, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    QAltResource1 = rabbit_misc:r(VHost1, queue, QAlt),
    QResource2 = rabbit_misc:r(VHost2, queue, Q),
    QAltResource2 = rabbit_misc:r(VHost2, queue, QAlt),
    
    %% Default vhost:
    %%    direct - queue
    %%    topic  - altqueue
    %% Vhost1:
    %%    direct - altqueue
    %% Vhost2:
    %%    topic  - queue
    %%    topic  - altqueue
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch1, #'queue.bind'{exchange = <<"amq.direct">>,
                                                              queue = QAlt,
                                                              routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch2, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch2, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DirectExchange1 = rabbit_misc:r(VHost1, exchange, <<"amq.direct">>),
    TopicExchange2 = rabbit_misc:r(VHost2, exchange, <<"amq.topic">>),
    DefaultExchange1 = rabbit_misc:r(VHost1, exchange, <<>>),
    DefaultExchange2 = rabbit_misc:r(VHost2, exchange, <<>>),

    %% Direct exchange on default vhost
    ?assertEqual(
       [binding_record(DirectExchange, QResource, Q, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange]))),
    %% Direct exchange on vhost 1
    ?assertEqual(
       [binding_record(DirectExchange1, QAltResource1, QAlt, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [DirectExchange1]))),
    ?assertEqual(
       lists:sort([binding_record(DefaultExchange1, QAltResource1, QAlt, []),
                   binding_record(DirectExchange1, QAltResource1, QAlt, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource1]))),
    %% Topic exchange on default vhost
    ?assertEqual(
       [binding_record(TopicExchange, QAltResource, QAlt, [])],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange]))),
    %% Topic exchange on vhost 2
    ?assertEqual(
       lists:sort([binding_record(TopicExchange2, QAltResource2, QAlt, []),
                   binding_record(TopicExchange2, QResource2, Q, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_source,
                                               [TopicExchange2]))),
    ?assertEqual(
       lists:sort([binding_record(TopicExchange2, QAltResource2, QAlt, []),
                   binding_record(DefaultExchange2, QAltResource2, QAlt, [])]),
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource2]))).

list_with_multiple_arguments(Config) ->
    %% Bindings are made of source, destination, routing key and arguments.
    %% Arguments are difficult to use on khepri paths and also are not relevant to any
    %% existing query. Thus, internally the bindings in Khepri are indexed using
    %% source, destination and key. Each entry on Khepri contains a set of bindings.
    %% For the `rabbit_binding` API nothing has changed, let's test here listing outputs
    %% with multiple arguments for the same source, destination and routing key.
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    
    %% Binding to the default exchange, it's always present
    ?assertEqual([DefaultBinding],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>])),
    
    %% Let's bind with multiple arguments
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q,
                                                             arguments = [{<<"x-match">>, longstr, <<"all">>}]}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.headers">>,
                                                             queue = Q,
                                                             routing_key = Q,
                                                             arguments = [{<<"x-match">>, longstr, <<"any">>}]}),
    
    AllBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                QResource, Q, [{<<"x-match">>, longstr, <<"all">>}]),
    AnyBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.headers">>),
                                QResource, Q, [{<<"x-match">>, longstr, <<"any">>}]),
    Bindings = lists:sort([DefaultBinding, AllBinding, AnyBinding]),

    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list, [<<"/">>]))),

    ok.

list_for_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    DirectABinding = binding_record(DirectExchange, QAltResource, QAlt, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    TopicABinding = binding_record(TopicExchange, QAltResource, QAlt, []),
    DefaultBinding = binding_record(DefaultExchange, QResource, Q, []),
    DefaultABinding = binding_record(DefaultExchange, QAltResource, QAlt, []),

    Bindings = lists:sort([DefaultBinding, DirectBinding, TopicBinding]),
    AltBindings = lists:sort([DefaultABinding, DirectABinding, TopicABinding]),
    
    ?assertEqual(
       Bindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QResource]))),
    ?assertEqual(
       AltBindings,
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_for_destination,
                                               [QAltResource]))).

list_for_source_and_destination(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    QAlt = ?config(alt_queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),
    ?assertEqual({'queue.declare_ok', QAlt, 0, 0}, declare(Ch, QAlt, [])),
    
    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    QAltResource = rabbit_misc:r(<<"/">>, queue, QAlt),
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.topic">>,
                                                             queue = QAlt,
                                                             routing_key = QAlt}),

    DirectExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
    TopicExchange = rabbit_misc:r(<<"/">>, exchange, <<"amq.topic">>),
    DefaultExchange = rabbit_misc:r(<<"/">>, exchange, <<>>),
    DirectBinding = binding_record(DirectExchange, QResource, Q, []),
    TopicBinding = binding_record(TopicExchange, QResource, Q, []),
    DefaultABinding = binding_record(DefaultExchange, QAltResource, QAlt, []),
    
    ?assertEqual(
       [DirectBinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [DirectExchange, QResource]))),
    ?assertEqual(
       [TopicBinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [TopicExchange, QResource]))),
    ?assertEqual(
       [DefaultABinding],
       lists:sort(rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding,
                                               list_for_source_and_destination,
                                               [DefaultExchange, QAltResource]))).

 list_explicit(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),

    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    QResource = rabbit_misc:r(<<"/">>, queue, Q),
    
    ?assertEqual([],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_explicit, [])),
    
    %% Let's bind to other exchanges
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.fanout">>,
                                                             queue = Q,
                                                             routing_key = Q}),
    
    DirectBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.direct">>),
                                     QResource, Q, []),
    FanoutBinding = binding_record(rabbit_misc:r(<<"/">>, exchange, <<"amq.fanout">>),
                                     QResource, Q, []),
    Bindings = lists:sort([DirectBinding, FanoutBinding]),
    
    ?assertEqual(Bindings,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, list_explicit, []))),
    
    ok.

info_all(Config) ->
    Server = rabbit_ct_broker_helpers:get_node_config(Config, 0, nodename),
    
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    Q = ?config(queue_name, Config),
    ?assertEqual({'queue.declare_ok', Q, 0, 0}, declare(Ch, Q, [])),

    Default = [{source_name,<<>>},
               {source_kind,exchange},
               {destination_name,<<"info_all">>},
               {destination_kind,queue},
               {routing_key,<<"info_all">>},
               {arguments,[]},
               {vhost,<<"/">>}],

    ?assertEqual([Default],
                 rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, info_all, [<<"/">>])),

    Direct = [{source_name,<<"amq.direct">>},
              {source_kind,exchange},
              {destination_name,<<"info_all">>},
              {destination_kind,queue},
              {routing_key,<<"info_all">>},
              {arguments,[]},
              {vhost,<<"/">>}],
    
    #'queue.bind_ok'{} = amqp_channel:call(Ch, #'queue.bind'{exchange = <<"amq.direct">>,
                                                             queue = Q,
                                                             routing_key = Q}),

    Infos = lists:sort([Default, Direct]),
    ?assertEqual(Infos,
                 lists:sort(
                   rabbit_ct_broker_helpers:rpc(Config, 0, rabbit_binding, info_all, [<<"/">>]))),
    
    ok.

delete_queues() ->
    [{ok, _} = rabbit_amqqueue:delete(Q, false, false, <<"dummy">>)
     || Q <- rabbit_amqqueue:list()].

declare(Ch, Q, Args) ->
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           auto_delete = false,
                                           arguments = Args}).

binding_record(Src, Dst, Key, Args) ->
    #binding{source = Src,
             destination = Dst,
             key = Key,
             args = Args}.
