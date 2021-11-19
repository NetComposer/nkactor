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

%% @doc NkACTOR Domain Application Module
-module(nkactor_app).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(application).

-export([start/0, start/1, start/2, stop/1]).
-export([metric_find/2, metric_read/2, metric_create/2, metric_update/2]).
-export([set_nodes/1]).
-export([get/1, get/2, put/2, del/1]).

-include("nkactor.hrl").

-define(APP, nkactor).
-compile({no_auto_import, [get/1, put/2]}).

%% ===================================================================
%% Private
%% ===================================================================

%% @doc Starts NkACTOR stand alone.
-spec start() -> 
    ok | {error, Reason::term()}.

start() ->
    start(permanent).


%% @doc Starts NkACTOR stand alone.
-spec start(permanent|transient|temporary) -> 
    ok | {error, Reason::term()}.

start(Type) ->
    case application:ensure_all_started(?APP, Type) of
        {ok, _Started} ->
            ok;
        Error ->
            Error
    end.


%% @doc
start(_Type, _Args) ->
    Syntax = #{
    },
    case nklib_config:load_env(?APP, Syntax) of
        {ok, _} ->
            {ok, Pid} = nkactor_sup:start_link(),
            {ok, Vsn} = application:get_key(nkactor, vsn),
            reg_metrics(),
            lager:info("NkACTOR v~s has started.", [Vsn]),
            {ok, Pid};
        {error, Error} ->
            lager:error("Error parsing config: ~p", [Error]),
            error(Error)
    end.


reg_metrics() ->
    prometheus_counter:declare([{name, actor_find}, {labels, [type]}, {help, "Actors for find"}]),
    prometheus_counter:declare([{name, actor_read}, {labels, [type]}, {help, "Actors for read"}]),
    prometheus_counter:declare([{name, actor_create}, {labels, [type]}, {help, "Actors for create"}]),
    prometheus_counter:declare([{name, actor_update}, {labels, [type]}, {help, "Actors for update"}]),
    ok.

metric_find(Group, Res) ->
    catch prometheus_counter:inc(actor_find, [<<Group/binary, $/, Res/binary>>]),
    ok.

metric_read(Group, Res) ->
    catch prometheus_counter:inc(actor_read, [<<Group/binary, $/, Res/binary>>]),
    ok.

metric_create(Group, Res) ->
    catch prometheus_counter:inc(actor_create, [<<Group/binary, $/, Res/binary>>]),
    ok.

metric_update(Group, Res) ->
    catch prometheus_counter:inc(actor_update, [<<Group/binary, $/, Res/binary>>]),
    ok.

%% @doc
set_nodes(Nodes) when is_list(Nodes) ->
    ?MODULE:put(nodes, [nklib_util:to_binary(Node) || Node <- Nodes]).



%% @private OTP standard stop callback
stop(_) ->
    ok.


%% @doc gets a configuration value
get(Key) ->
    get(Key, undefined).


%% @doc gets a configuration value
get(Key, Default) ->
    nklib_config:get(?APP, Key, Default).


%% @doc updates a configuration value
put(Key, Value) ->
    nklib_config:put(?APP, Key, Value).


%% @doc updates a configuration value
del(Key) ->
    nklib_config:del(?APP, Key).
