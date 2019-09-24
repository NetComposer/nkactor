
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

%% @doc NkACTOR service master process
%% - it bases itself on the standard nkserver master process,
%%   being called from nkactor_callbacks:srv_master_...()
%% - it will check if the 'base_namespace' is started (at all master processes)
%%   if it is the leader, it will start and monitor it, and restart if necessary
%% - when a namespace starts, it registers with the leader of the service
%%   that manages this namespace's registered service,
%%   calling register_namespace/2
%%   (see nkactor_namespace:init/1)

%% - the leader registers and monitors the namespace's processes
%% - every instance at each node (not only leaders) checks periodically
%%   that the 'base' namespace for the service
%%   is started, and belong to this service
%% - it also performs auto-activation of actors


-module(nkactor_master).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([register_namespace/2, get_namespace/2]).
-export([get_namespaces/1, get_namespaces/2, get_all_namespaces/0, get_all_namespaces/1]).
-export([stop_all_namespaces/2]).
-export([srv_master_init/2, srv_master_handle_call/4,
         srv_master_handle_cast/3, srv_master_handle_info/3,
         srv_master_timed_check/3]).

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkACTOR Master (~s) "++Txt, [maps:get(base_namespace, State)|Args])).


%% ===================================================================
%% Types
%% ===================================================================

-type id() :: nkactor:id().


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Registers a namespace
-spec register_namespace(id(), nkactor:namespace()) ->
    {ok, pid()} | {error, nkserver:status()}.

register_namespace(SrvId, Namespace) when is_binary(Namespace) ->
    nkserver_master:call_leader(SrvId, {nkactor_register_namespace, Namespace, self()}).


%% @doc Gets a namespace processor
-spec get_namespace(id(), nkactor:namespace()) ->
    {ok, pid()} | {error, nkserver:status()}.

get_namespace(SrvId, Namespace) when is_binary(Namespace) ->
    nkserver_master:call_leader(SrvId, {nkactor_get_namespace, Namespace}).


%%%% @doc
%%-spec get_namespaces(id()) ->
%%    {ok, [{nkactor:namespace(), pid()]} | {error, term()}.

get_namespaces(SrvId) ->
    get_namespaces(SrvId, <<>>).

get_namespaces(SrvId, Namespace) ->
    case nkserver_master:call_leader(SrvId, {nkactor_get_namespaces, Namespace}) of
        {ok, NS} ->
            {ok, [{Name, Pid} || {_Parts, Name, Pid} <- NS]};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Finds all namespaces for all services
get_all_namespaces() ->
    get_all_namespaces(<<>>).


%% @doc Finds all namespaces for all services under a base
get_all_namespaces(Namespace) ->
    Services = nkactor:get_services(),
    NS1 = lists:foldl(
        fun(SrvId, Acc) ->
            case nkserver_master:call_leader(SrvId, {nkactor_get_namespaces, Namespace}) of
                {ok, N1} ->
                    N2 = [{Path, N, SrvId, Pid} || {Path, N, Pid} <- N1],
                    [N2|Acc];
                {error, _Error} ->
                    Acc
            end
        end,
        [],
        Services),
    NS2 = lists:sort(lists:flatten(NS1)),
    [{N, SrvId, Pid} || {_Path, N, SrvId, Pid} <- NS2].


%% @doc Stops all namespaces under a base, first the deep ones
%% If reason is 'normal' childs will not restart automatically
stop_all_namespaces(Namespace, Reason) ->
    lists:foreach(
        fun({_Name, _SrvId, Pid}) -> gen_server:cast(Pid, {nkactor_stop, Reason}) end,
        lists:reverse(get_all_namespaces(Namespace))).




%% ===================================================================
%% Private
%% ===================================================================


-type state() ::
    #{
        base_namespace := nkactor:namespace(),
        base_namespace_pid := pid() | undefined,
        namespaces := [{[binary()], nkactor:namespace(), pid()}],
        next_auto_activate := integer()
    }.


%% @private
-spec srv_master_init(nkserver:id(), state()) ->
    {continue, [{nkserver:id(), state()}]}.

srv_master_init(SrvId, State) ->
    NextActivate = case nkserver:get_config(SrvId) of
        #{auto_activate_actors_period:=Time} ->
            nklib_date:epoch(msecs) + Time;
        _ ->
            0
    end,
    State2 = State#{
        base_namespace => nkactor:base_namespace(SrvId),
        base_namespace_pid => undefined,
        namespaces => [],
        next_auto_activate => NextActivate
    },
    gen_server:cast(self(), nkactor_check_base_namespace),
    {continue, [SrvId, State2]}.


%% @private
srv_master_handle_call({nkactor_get_namespaces, <<>>}, _From, _SrvId, State) ->
    #{namespaces := Namespaces} = State,
    {reply, {ok, Namespaces}, State};

srv_master_handle_call({nkactor_get_namespaces, Namespace}, _From, _SrvId, State) ->
    #{namespaces := NS1} = State,
    Base = nkactor_lib:make_rev_parts(Namespace),
    NS2 = lists:filter(
        fun({Path, _N, _Pid}) -> lists:prefix(Base, Path) end,
        NS1),
    {reply, {ok, NS2}, State};

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
srv_master_handle_cast(nkactor_check_base_namespace, _SrvId, State) ->
    {noreply, check_base_namespace(false, State)};

srv_master_handle_cast(_Msg,  _SrvId, _State) ->
    continue.


%% @private
srv_master_handle_info({'DOWN', _Ref, process, Pid, _Reason}, _SrvId, State) ->
    case State of
        #{base_namespace_pid := Pid} ->
            ?LLOG(notice, "base namespace has failed!", [], State),
            State2 = State#{base_namespace_pid := undefined},
            % We may not be leaders any more
            State3 = check_base_namespace(false, State2),
            {noreply, State3};
        _ ->
            case do_remove_namespace(Pid, State) of
                {true, State2} ->
                    {noreply, State2};
                false ->
                    continue
            end
    end;

srv_master_handle_info(_Msg, _SrvId, _State) ->
    continue.


%% @private
srv_master_timed_check(IsLeader, SrvId, State) ->
    State2 = check_base_namespace(IsLeader, State),
    State3 = check_auto_activate(SrvId, State2),
    {continue, [IsLeader, SrvId, State3]}.


%% ===================================================================
%% Register
%% ===================================================================

%% @private
%% All processes master (leader or not) check that base namespace is started
%% The leader also monitors it, to restart it immediately
check_base_namespace(IsLeader, #{base_namespace:=Namespace}=State) ->
    case nkactor_namespace:get_global_pid(Namespace) of
        Pid when is_pid(Pid), IsLeader ->
            case State of
                #{base_namespace_pid:=Pid} ->
                    State;
                _ ->
                    monitor(process, Pid),
                    State#{base_namespace_pid := Pid}
            end;
        Pid when is_pid(Pid) ->
            State;
        undefined ->
            spawn_link(fun() -> nkactor_namespace:start(Namespace) end),
            State
    end.


%% @private
check_auto_activate(SrvId, #{next_auto_activate:=Next}=State) when Next > 0 ->
    Now = nklib_date:epoch(msecs),
    case Now > Next of
        true ->
            spawn(fun() -> nkactor_util:activate_actors(SrvId, 2*60*60*1000) end),
            #{auto_activate_actors_period:=Time} = nkserver:get_config(SrvId),
            State#{next_auto_activate := Now+Time};
        false ->
            State
    end;

check_auto_activate(_SrvId, State) ->
    State.


%% @private
do_register_namespace(Namespace, Pid, State) ->
    #{base_namespace:=Base, namespaces:=Namespaces} = State,
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
                    State2 = State#{namespaces:=Namespaces2},
                    {ok, State2}
            end;
        _ ->
            ?LLOG(warning, "invalid namespace ~s for base ~s", [Namespace, Base], State),
            {error, service_invalid}
    end.


%% @private
do_find_namespace(Namespace, #{namespaces:=Namespaces}) ->
    case lists:keyfind(Namespace, 2, Namespaces) of
        {_, _, Pid} ->
            {true, Pid};
        false ->
            false
    end.


%% @private
do_remove_namespace(Pid, #{namespaces:=Namespaces}=State) ->
    case lists:keytake(Pid, 3, Namespaces) of
        {value, _, Namespaces2} ->
            {true, State#{namespaces:=Namespaces2}};
        false ->
            false
    end.

