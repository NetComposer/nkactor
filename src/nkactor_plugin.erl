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
-export([plugin_deps/0, plugin_config/3, plugin_cache/3]).
-export_type([continue/0]).

-type continue() :: continue | {continue, list()}.

-include("nkactor.hrl").

%% ===================================================================
%% Plugin Callbacks
%% ===================================================================


%% @doc 
plugin_deps() ->
	[].


%% @doc 
plugin_config(SrvId, Config, #{class:=?PACKAGE_CLASS_NKACTOR}) ->
    Syntax = #{
        base_namespace => binary,
        persistence_module => module,
        debug => boolean,
        debug_actors => {list, binary},
        '__defaults' => #{
            base_namespace => nklib_util:to_binary(SrvId)
        }
    },
    nkserver_util:parse_config(Config, Syntax).


%% @doc 
plugin_cache(_SrvId, Config, _Service) ->
    Cache = #{
        base_namespace => maps:get(base_namespace, Config),
        persistence_module => maps:get(persistence_module, Config, undefined),
        debug => maps:get(debug, Config, false),
        debug_actors => maps:get(debug_actors, Config, [])
    },
    {ok, Cache}.
