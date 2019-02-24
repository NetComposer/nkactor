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

-export([register_modules/2, get_module/2]).
-export([get_services/0]).
-export([get_actor_config/1]).

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
        KeyList).


%% @doc Gets the callback module for an actor resource or type
-spec get_module(group(), resource()|{singular, binary()}|{camel, binary()}|{short, binary()}) ->
    module() | undefined.

get_module(Group, Key) ->
    nklib_util:do_config_get({nkactor_module, to_bin(Group), to_bin(Key)}).

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
                    case catch nklib_util:do_config_get({nkactor_config, SrvId, Module}) of
                        undefined ->
                            Config1 = nkactor_actor:config(Module),
                            Config2 = ?CALL_SRV(SrvId, actor_config, [Config1]),
                            Config3 = Config2#{module=>Module},
                            nklib_util:do_config_put({nkactor_config, SrvId, Module}, Config2),
                            {ok, Config3};
                        Config when is_map(Config) ->
                            {ok, Config}
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).