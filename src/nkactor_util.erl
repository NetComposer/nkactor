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

-export([register_modules/2, get_module/2, get_modules/0]).
-export([get_services/0]).
-export([get_actor_config/1, get_actor_config/2, get_actor_config/3]).
-export([pre_create/3, pre_update/4]).

-type group() :: nkactor:group().
-type resource() :: nkactor:resource().

%% ===================================================================
%% Public
%% ===================================================================

%% @doc Register actor modules for a group
%% For each actor, it registers:
%% - resource (binary())
%% - {singular, binary()}
%% - {camel, binary()}
%% - {short, binary()} (can be several)
-spec register_modules(group(), [module()]) ->
    ok.

register_modules(Group, Modules) ->
    KeyList = lists:foldl(
        fun(Mod, Acc) ->
            Config = nkactor_actor:config(Mod),
            #{
                resource := Res,
                singular := Singular,
                camel := Camel,
                short_names := Short
            } = Config,
            Acc2 = [
                {to_bin(Res), Mod},
                {{singular, Singular}, Mod},
                {{camel, Camel}, Mod}
                | Acc
            ],
            lists:foldl(
                fun(SN, AccSN) -> [{{short, to_bin(SN)}, Mod}|AccSN] end,
                Acc2,
                Short)
        end,
        [],
        Modules),
    lists:foreach(
        fun({Key, Mod}) ->
            nklib_util:do_config_put({nkactor_module, Group, Key}, Mod)
        end,
        KeyList),
    Modules1 = get_modules(),
    Modules2 = lists:usort(Modules1++Modules),
    nklib_util:do_config_put(nkactor_modules, Modules2).



%% @doc Gets the callback module for an actor resource or type
-spec get_module(group(), resource()|{singular, binary()}|{camel, binary()}|{short, binary()}) ->
    module() | undefined.

get_module(Group, Key) ->
    nklib_util:do_config_get({nkactor_module, to_bin(Group), to_bin(Key)}).


%% @doc Gets all of the registered actor callback modules
-spec get_modules() ->
    [module()].

get_modules() ->
    nklib_util:do_config_get(nkactor_modules, []).



%% @doc
get_services() ->
    [
        SrvId ||
        {SrvId, _Hash, _Pid} <- nkserver_srv:get_all_local(?PACKAGE_CLASS_NKACTOR)
    ].


%% @doc Used to get modified configuration for the service responsible
get_actor_config(ActorId) ->
    #actor_id{group=Group, resource=Resource, namespace=Namespace} = ActorId,
    case get_module(Group, Resource) of
        undefined ->
            {error, resource_invalid};
        Module ->
            case nkactor_namespace:get_namespace(Namespace) of
                {ok, SrvId, _} ->
                    get_actor_config(SrvId, Module);
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc Used to get modified configuration for the service responsible
get_actor_config(SrvId, Group, Resource) ->
    Module = get_module(Group, Resource),
    get_actor_config(SrvId, Module).


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
                    Module = nkactor_util:get_module(Group, Res),
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
                    Module = nkactor_util:get_module(Group, Res),
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


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).