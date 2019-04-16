%% -------------------------------------------------------------------
%%
%% Copyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------


%% @doc Basic Actor utilities
-module(nkactor_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-export([get_services/0]).
-export([get_module/3]).
-export([get_actor_config/1, get_actor_config/2, get_actor_config/3]).
-export([pre_create/3, pre_update/4]).
-export([activate_actors/1]).
-export([trace_create/0, trace_on/0, trace_off/0, trace_insert/2,
         trace_insert_traces/1, trace_dump/0]).

-type group() :: nkactor:group().
-type resource() :: nkactor:resource().

-define(ACTIVATE_SPAN, auto_activate).


%% ===================================================================
%% Public
%% ===================================================================

%% @doc
get_services() ->
    [
        SrvId ||
        {SrvId, _Hash, _Pid} <- nkserver_srv:get_all_local(?PACKAGE_CLASS_NKACTOR)
    ].


%% @doc Gets the callback module for an actor resource or type
-spec get_module(nkserver:id(), group(), resource()|{singular, binary()}|{camel, binary()}|{short, binary()}) ->
    module() | undefined.

get_module(SrvId, Group, Key) ->
    nkserver:get_cached_config(SrvId, nkactor, {module, to_bin(Group), Key}).


%% @doc Used to get modified configuration for the service responsible
get_actor_config(ActorId) ->
    #actor_id{group=Group, resource=Resource, namespace=Namespace} = ActorId,
    case nkactor_namespace:get_namespace(Namespace) of
        {ok, SrvId, _} ->
            case get_module(SrvId, Group, Resource) of
                undefined ->
                    {error, resource_invalid};
                Module ->
                    {ok, Config} = get_actor_config(SrvId, Module),
                    {ok, SrvId, Config}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Used to get modified configuration for the service responsible
get_actor_config(SrvId, Group, Resource) ->
    case get_module(SrvId, Group, Resource) of
        undefined ->
            {error, resource_invalid};
        Module ->
            get_actor_config(SrvId, Module)
    end.


%% @doc Used to get modified configuration for the service responsible
get_actor_config(SrvId, Module) when is_atom(SrvId), is_atom(Module) ->
    case catch nklib_util:do_config_get({nkactor_config, SrvId, Module}) of
        undefined ->
            Config1 = nkactor_actor:config(Module),
            Config2 = ?CALL_SRV(SrvId, actor_config, [Config1]),
            Config3 = Config2#{module=>Module},
            nklib_util:do_config_put({nkactor_config, SrvId, Module}, Config2),
            {ok, Config3};
        Config when is_map(Config) ->
            {ok, Config}
    end.


%% @private
pre_create(Actor, Syntax, Opts) ->
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            Actor3 = nkactor_lib:add_creation_fields(Actor2),
            Actor4 = case Opts of
                #{forced_uid:=UID} ->
                    Actor3#{uid := UID};
                _ ->
                    Actor3
            end,
            #{group:=Group, resource:=Res, namespace:=Namespace} = Actor4,
            case nkactor_namespace:get_namespace(Namespace) of
                {ok, SrvId, _Pid} ->
                    Req1 = maps:get(request, Opts, #{}),
                    Req2 = Req1#{
                        verb => create,
                        srv => SrvId
                    },
                    Module = nkactor_util:get_module(SrvId, Group, Res),
                    case nkactor_actor:parse(Module, Actor4, Req2) of
                        {ok, Actor5} ->
                            case nkactor_lib:check_links(Actor5) of
                                {ok, Actor6} ->
                                    {ok, SrvId, Actor6};
                                {error, Error} ->
                                    {error, Error}
                            end;
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @private
pre_update(ActorId, Syntax, Actor, Opts) ->
    #actor_id{group=Group, resource=Res, namespace=Namespace} = ActorId,
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            case nkactor_namespace:get_namespace(Namespace) of
                {ok, SrvId, _Pid} ->
                    Req1 = maps:get(request, Opts, #{}),
                    Req2 = Req1#{
                        verb => update,
                        srv => SrvId
                    },
                    Module = nkactor_util:get_module(SrvId, Group, Res),
                    case nkactor_actor:parse(Module, Actor2, Req2) of
                        {ok, Actor3} ->
                            case nkactor_lib:check_links(Actor3) of
                                {ok, Actor4} ->
                                    {ok, SrvId, Actor4};
                                {error, Error} ->
                                    {error, Error}
                            end;
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Performs an query on database for actors marked as 'active' and tries
%% to active them if not already activated
activate_actors(SrvId) ->
    nkserver_ot:new(?ACTIVATE_SPAN, SrvId, <<"Actor::auto-activate">>),
    activate_actors(SrvId, <<>>),
    nkserver_ot:finish(?ACTIVATE_SPAN),
    ok.


%% @private
activate_actors(SrvId, StartCursor) ->
    nkserver_ot:log(?ACTIVATE_SPAN, {"starting cursor: ~s", [StartCursor]}),
    ParentSpan = nkserver_ot:get_parent(?ACTIVATE_SPAN),
    case nkactor:search_active(SrvId, #{last_cursor=>StartCursor, parent_span=>ParentSpan, size=>2}) of
        {ok, [], _} ->
            nkserver_ot:log(?ACTIVATE_SPAN, <<"no more actors">>),
            ok;
        {ok, ActorIds, #{last_cursor:=LastDate}} ->
            nkserver_ot:log(?ACTIVATE_SPAN, {"found '~p' actors", [length(ActorIds)]}),
            lists:foreach(
                fun(ActorId) ->
                    case nkactor_namespace:find_registered_actor(ActorId) of
                        {true, _, _} ->
                            ok;
                        _ ->
                            case nkactor:activate(ActorId) of
                                {ok, _} ->
                                    nkserver_ot:log(?ACTIVATE_SPAN, {"activated actor ~p", [ActorId]}),
                                    lager:notice("NkACTOR auto-activating ~p", [ActorId]);
                                {error, Error} ->
                                    nkserver_ot:log(?ACTIVATE_SPAN, {"could not activated actor ~p: ~p",
                                                    [ActorId, Error]}),
                                    nkserver_ot:tag_error(?ACTIVATE_SPAN, could_not_activate_actor),
                                    lager:warning("NkACTOR could not auto-activate ~p: ~p",
                                                 [ActorId, Error])
                            end
                    end
                end,
                ActorIds),
            activate_actors(SrvId, LastDate);
        {error, Error} ->
            {error, Error}
    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).






trace_create() ->
    catch ets:new(nkactor_trace, [ordered_set, public, named_table]).


trace_on() ->
    put(nkserver_trace, true).


trace_off() ->
    put(nkserver_trace, false).


trace_insert(Id, Meta) ->
    case get(nkserver_trace) of
        true ->
            Time = nklib_date:epoch(usecs),
            Pos = erlang:unique_integer([positive, monotonic]),
            ets:insert(nkactor_trace, {{Time, Pos}, Id, Meta});
        _ ->
            ok
    end.


trace_insert_traces(Traces) ->
    case get(nkserver_trace) of
        true ->
            ets:insert(nkactor_trace, Traces);
        false ->
            ok
    end.


trace_dump() ->
    Traces = ets:tab2list(nkactor_trace),
    ets:delete_all_objects(nkactor_trace),
    Traces.



