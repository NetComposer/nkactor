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

%% @doc Default callbacks for plugin definitions
-module(nkactor_plugin).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([add_modules/3]).
-export([plugin_deps/0, plugin_meta/0, plugin_config/3, plugin_cache/3]).
-export([plugin_start/3, plugin_stop/3, plugin_update/4]).
-export_type([continue/0]).

-type continue() :: continue | {continue, list()}.

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").


%% ===================================================================
%% Public
%% ===================================================================

%% @doc Called normally from plugin_config to add group modules
add_modules(Config, Group, Modules) ->
    GroupModules1 = maps:get(modules, Config, #{}),
    GroupModules2 = GroupModules1#{Group => Modules},
    {ok, Config#{modules => GroupModules2}}.


%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc 
plugin_deps() ->
	[].


%% @doc
%% This service uses the 'master' facilities from nkserver,
%% starting a master server (see nkactor_master)
plugin_meta() ->
    #{use_master => true}.


%% @doc 
plugin_config(SrvId, Config, #{class:=nkactor}) ->
    Syntax = #{
        base_namespace => binary,
        auto_activate_actors_period => {integer, 5000, none},
        debug => boolean,
        debug_actors => {list, binary},
        modules => #{'__map_binary' => {list, module}}, % To be populated by plugins
        '__defaults' => #{
            base_namespace => nklib_util:to_binary(SrvId)
        }
    },
    nkserver_util:parse_config(Config, Syntax).


%% @doc
%% Insert basic entries in cache
%% Also, for each module, entries are added pointing to each module:
%% - {module, Group::binary(), Resource::binary()} -> module()
%% - {module, Group, {singular, Singular::binary()} -> module()
%% - {module, Group, {camel, Camel::binary()}} -> module()
%% - {module, Group, {short, Short::binary()} -> module() (can be several)
%%
%% It also generated the function 'nkactor_callback' after exported actors' functions
%% -nkactor_callback(sync_op, Group, Res, Args) ->
%%    apply(nkactor_core_..., sync_op, Args)

plugin_cache(SrvId, Config, _Service) ->
    Cache1 = #{
        base_namespace => maps:get(base_namespace, Config),
        debug => maps:get(debug, Config, false),
        debug_actors => maps:get(debug_actors, Config, [])
    },
    Modules = maps:get(modules, Config, #{}),
    Cache2 = maps:fold(
        fun(Group, ModList, Acc) -> expand_modules(Group, ModList, Acc) end,
        Cache1,
        Modules),
    Callbacks = gen_actor_callbacks(SrvId, Modules),
    {ok, Cache2, [{nkactor_callback, 4, Callbacks}]}.


plugin_start(SrvId, #{base_namespace:=Namespace}, _Service) ->
    nklib_config:put(nkactor_namespace, Namespace, SrvId),
    ?CALL_SRV(SrvId, actor_plugin_init, [SrvId]).


plugin_stop(_SrvId, #{base_namespace:=Namespace}, _Service) ->
    nklib_config:del(nkactor_namespace, Namespace),
    ok.

plugin_update(SrvId, #{base_namespace:=New}, #{base_namespace:=Old}, _Service) ->
    nklib_config:del(nkactor_namespace, Old),
    nklib_config:put(nkactor_namespace, New, SrvId),
    ok.




%% ===================================================================
%% Internal
%% ===================================================================

%% @doc Register actor modules for a group
%% For each actor, it registers:
expand_modules(Group, Modules, Config) ->
    KeyList = lists:foldl(
        fun(Mod, Acc) ->
            #{resource:=Res1} = ModConfig = Mod:config(),
            Res2 = nklib_util:to_binary(Res1),
            Singular = case ModConfig of
                #{singular:=S0} ->
                    nklib_util:to_binary(S0);
                _ ->
                    nkactor_lib:make_singular(Res2)
            end,
            Camel = case ModConfig of
                #{camel:=C0} ->
                    nklib_util:to_binary(C0);
                _ ->
                    nklib_util:to_capital(Singular)
            end,
            ShortNames = case ModConfig of
                #{short_names:=SN} ->
                    [nklib_util:to_binary(N) || N <- SN];
                _ ->
                    []
            end,
            Acc2 = [
                {Res2, Mod},
                {{singular, Singular}, Mod},
                {{camel, Camel}, Mod}
                | Acc
            ],
            lists:foldl(
                fun(SN, AccSN) -> [{{short, nklib_util:to_binary(SN)}, Mod}|AccSN] end,
                Acc2,
                ShortNames)
        end,
        [],
        Modules),
    lists:foldl(
        fun({Key, Mod}, Acc2) -> Acc2#{{module, Group, Key} => Mod} end,
        Config,
        KeyList).



%% @doc Generates a fun called 'nkactor_callback' after exported functions in
%% all defined actor callback modules:
%%
%% nkactor_callback(Group, Res, Fun, Args) -> apply(ActorMod, Fun, Args);
%% ...
%% nkactor_callbacks(_, _, _, _) -> continue.
%%
gen_actor_callbacks(SrvId, Modules) ->
    Callbacks1 = lists:map(
        fun({Group, ModList}) ->
            gen_actor_callbacks(SrvId, Group, ModList, [])
        end,
        maps:to_list(Modules)),
    Callbacks2 = lists:sort(lists:flatten(Callbacks1)),
    Clauses = lists:map(
        fun({Group, Res, Mod, Fun, _Arity}) ->
            erl_syntax:clause(
                [
                    erl_syntax:atom(Fun),
                    erl_syntax:abstract(Group),
                    erl_syntax:abstract(Res),
                    erl_syntax:variable("Args")
                ],
                [],
                [
                    erl_syntax:application(
                        erl_syntax:atom(apply),
                        [
                            erl_syntax:atom(Mod),
                            erl_syntax:atom(Fun),
                            erl_syntax:variable("Args")
                        ]
                    )
                ]
            )
        end,
        Callbacks2)
        ++ [
            erl_syntax:clause(
                [
                    erl_syntax:variable("_"),
                    erl_syntax:variable("_"),
                    erl_syntax:variable("_"),
                    erl_syntax:variable("_")
                ],
                [],
                [
                    erl_syntax:atom(continue)
                ]
            )
        ],
    Fun = erl_syntax:function(
        erl_syntax:atom(nkactor_callback),
        Clauses),
    erl_syntax:revert(Fun).



%% @private
gen_actor_callbacks(_SrvId, _Group, [], Acc) ->
    Acc;

gen_actor_callbacks(SrvId, Group, [Module|Rest], Acc) ->
    #{resource:=Res} = Module:config(),
    FunList = [
        {parse, 3},
        {unparse, 2},
        {get_labels, 1},
        {request, 4},
        {save, 2},
        {init, 2},
        {get, 2},
        {update, 3},
        {delete, 2},
        {sync_op, 3},
        {async_op, 2},
        {enabled, 2},
        {heartbeat, 1},
        {event, 3},
        {link_event, 5},
        {activated, 2},
        {expired, 2},
        {handle_call, 3},
        {handle_cast, 2},
        {handle_info, 2},
        {stop, 2},
        {terminate, 2}
    ],
    Acc3 = lists:foldl(
        fun({Fun, Arity}, Acc2) ->
            case erlang:function_exported(Module, Fun, Arity) of
                true ->
                    [{Group, Res, Module, Fun, Arity}|Acc2];
                false ->
                    Acc2
            end
        end,
        Acc,
        FunList),
    gen_actor_callbacks(SrvId, Group, Rest, Acc3).






