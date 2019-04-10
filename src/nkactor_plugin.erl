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
-export([plugin_deps/0, plugin_config/3, plugin_cache/3]).
-export_type([continue/0]).

-type continue() :: continue | {continue, list()}.

-include("nkactor.hrl").



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
plugin_config(SrvId, Config, #{class:=?PACKAGE_CLASS_NKACTOR}) ->
    Syntax = #{
        modules => #{'__key_binary' => {list, module}},
        base_namespace => binary,
        persistence_module => module,
        auto_activate_actors_period => {integer, 5000, none},
        debug => boolean,
        debug_actors => {list, binary},
        '__defaults' => #{
            base_namespace => nklib_util:to_binary(SrvId)
        }
    },
    nkserver_util:parse_config(Config, Syntax).


%% @doc
%% Insert basic entries in cache
%% Also, for each module, entries are added pointing to each module:
%% - {module, Group::binary(), Resource::binary()}
%% - {module, Group, {singular, Singular::binary()}
%% - {module, Group, {camel, Camel::binary()}}
%% - {module, Group, {short, Short::binary()} (can be several)
%%
%% It also generated the function 'nkactor_callback' after exported actors' functions
plugin_cache(SrvId, Config, _Service) ->
    Cache1 = #{
        base_namespace => maps:get(base_namespace, Config),
        persistence_module => maps:get(persistence_module, Config, undefined),
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




%% ===================================================================
%% Internal
%% ===================================================================

%% @doc Register actor modules for a group
%% For each actor, it registers:
expand_modules(Group, Modules, Config) ->
    KeyList = lists:foldl(
        fun(Mod, Acc) ->
            ModConfig = nkactor_actor:config(Mod),
            #{
                resource := Res,
                singular := Singular,
                camel := Camel,
                short_names := Short
            } = ModConfig,
            Acc2 = [
                {nklib_util:to_binary(Res), Mod},
                {{singular, Singular}, Mod},
                {{camel, Camel}, Mod}
                | Acc
            ],
            lists:foldl(
                fun(SN, AccSN) -> [{{short, nklib_util:to_binary(SN)}, Mod}|AccSN] end,
                Acc2,
                Short)
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
        {parse, 2},
        {request, 4},
        {save, 2},
        {init, 2},
        {get, 2},
        {update, 2},
        {delete, 1},
        {sync_op, 3},
        {async_op, 2},
        {enabled, 2},
        {heartbeat, 1},
        {event, 2},
        {link_event, 4},
        {next_status_timer, 1},
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






