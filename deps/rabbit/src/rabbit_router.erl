%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_router).
-include_lib("rabbit_common/include/rabbit.hrl").

-export([match_bindings/2, match_routing_key/2]).

%%----------------------------------------------------------------------------

-export_type([routing_key/0, match_result/0]).

-type routing_key() :: binary().
-type match_result() :: [rabbit_types:binding_destination()].

%%----------------------------------------------------------------------------

-spec match_bindings(rabbit_types:binding_source(),
                     fun ((rabbit_types:binding()) -> boolean())) ->
    match_result().

match_bindings(SrcName, Match) ->
    Bindings = ets:lookup(rabbit_route_index_2, SrcName),
    lists:filtermap(
      fun(#binding{destination = Destination} = Binding) ->
              case Match(Binding) of
                  true -> {true, Destination};
                  false -> false
              end
      end, Bindings).

-spec match_routing_key(rabbit_types:binding_source(),
                        [routing_key(), ...] | ['_']) ->
    match_result().

match_routing_key(SrcName, ['_']) ->
    %% The '_' wildcard got used in an older version of this function
    %% in the match specification of ets:select/2.
    destinations(SrcName);
match_routing_key(SrcName, [RoutingKey]) ->
    %% optimization
    destinations(SrcName, RoutingKey);
match_routing_key(SrcName, [_|_] = RoutingKeys) ->
    %%TODO filter out duplicate destinations?
    lists:foldl(fun(RKey, Acc) ->
                        destinations(SrcName, RKey) ++ Acc
                end, [], RoutingKeys).

destinations(SrcName) ->
    ets:lookup_element(rabbit_route_index_2, SrcName, 4).

destinations(SrcName, RoutingKey) ->
    ets:lookup_element(rabbit_route_index_1, {SrcName, RoutingKey}, 3).
