
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

%% @doc
-module(nkactor_master).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([register_namespace/2, get_namespace/2, get_namespaces/1]).
-export([srv_master_init/2, srv_master_handle_call/4, srv_master_handle_info/3,
         srv_master_timed_check/3]).

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(LLOG(Type, Txt, Args, State), lager:Type("NkACTOR Master "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type id() :: nkactor:id().


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Registers a namespace
-spec register_namespace(id(), nkactor:namespace()) ->
    {ok, pid()} | {error, nkserver:msg()}.

register_namespace(SrvId, Namespace) when is_binary(Namespace) ->
    nkserver_master:call_leader(SrvId, {nkactor_register_namespace, Namespace, self()}).


%% @doc Gets a namespace
-spec get_namespace(id(), nkactor:namespace()) ->
    {ok, pid()} | {error, nkserver:msg()}.

get_namespace(SrvId, Namespace) when is_binary(Namespace) ->
    nkserver_master:call_leader(SrvId, {nkactor_get_namespace, Namespace}).


%%%% @doc
%%-spec get_namespaces(id()) ->
%%    {ok, [{nkactor:namespace(), pid()]} | {error, term()}.

get_namespaces(SrvId) ->
    nkserver_master:call_leader(SrvId, nkactor_get_namespaces).



%% ===================================================================
%% Private
%% ===================================================================


-type state() ::
    #{
        nkactor_base_namespace => nkactor:namespace(),
        nkactor_namespaces => [{nkactor:namespace(), pid()}]
    }.


%% @private
-spec srv_master_init(nkserver:id(), state()) ->
    {continue, [{nkserver:id(), state()}]}.

srv_master_init(SrvId, State) ->
    BaseNamespace = nkserver:get_plugin_config(SrvId, nkactor, base_namespace),
    State2 = State#{
        nkactor_base_namespace => BaseNamespace,
        nkactor_namespaces => []
    },
    {continue, [SrvId, State2]}.


%% @private
srv_master_handle_call(nkactor_get_namespaces, _From, _SrvId, State) ->
    #{nkactor_namespaces := Namespaces} = State,
    {reply, {ok, Namespaces}, State};

srv_master_handle_call({nkactor_register_namespace, Namespace, Pid}, _From, _SrvId, State) ->
    case do_register_namespace(Namespace, Pid, State) of
        {ok, State2} ->
            {reply, {ok, self()}, State2};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

srv_master_handle_call({nkactor_get_namespace, Namespace}, _From, _SrvId, State) ->
    Reply = do_find_namespace(Namespace, State),
    {reply, Reply, State};

srv_master_handle_call(_Msg, _From, _SrvId, _State) ->
    continue.


%% @private
srv_master_handle_info({'DOWN', _Ref, process, Pid, _Reason}, _SrvId, State) ->
    case do_remove_namespace(Pid, State) of
        {true, State2} ->
            {noreply, State2};
        false ->
            continue
    end;

srv_master_handle_info(_Msg,  _SrvId, _State) ->
    continue.


srv_master_timed_check(_IsMaster, SrvId, State) ->
    check_base_namespace(SrvId, State),
    continue.


%% ===================================================================
%% Register
%% ===================================================================

%% @private
check_base_namespace(SrvId, #{nkactor_base_namespace:=Namespace}) ->
    case nkactor_namespace:get_pid(Namespace) of
        Pid when is_pid(Pid) ->
            ok;
        undefined ->
            spawn_link(fun() -> nkactor_namespace:start(SrvId, Namespace) end)
    end.


do_register_namespace(Namespace, Pid, State) ->
    #{nkactor_base_namespace:=Base, nkactor_namespaces:=Namespaces} = State,
    case binary:match(Namespace, Base) of
        {S, T} when S+T == byte_size(Namespace) ->
            case lists:keyfind(Namespace, 2, Namespaces) of
                {_, _, Pid} ->
                    {ok, State};
                {Namespace, OldPid} ->
                    {error, {already_registered, OldPid}};
                false ->
                    monitor(process, Pid),
                    Parts = lists:reverse(binary:split(Namespace, <<".">>, [global])),
                    Namespaces2 = lists:sort([{Parts, Namespace, Pid}|Namespaces]),
                    State2 = State#{nkactor_namespaces := Namespaces2},
                    {ok, State2}
            end;
        _ ->
            ?LLOG(warning, "invalid namespace ~s for base ~s", [Namespace, Base], State),
            {error, service_invalid}
    end.


%% @private
do_find_namespace(Namespace, #{nkactor_namespaces:=Namespaces}) ->
    case lists:keyfind(Namespace, 2, Namespaces) of
        {_, _, Pid} ->
            {true, Pid};
        false ->
            false
    end.


%% @private
do_remove_namespace(Pid, #{nkactor_namespaces:=Namespaces}=State) ->
    case lists:keytake(Pid, 3, Namespaces) of
        {value, _, Namespaces2} ->
            {true, State#{nkactor_namespaces := Namespaces2}};
        false ->
            false
    end.
