
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
%% - it bases itself on the standard nkserver master process
%% - when a namespace starts, it registers with the leader of the master
%%   process that belongs to the namespace's registered service,
%%   calling register_namespace/2
%% - the leader registers and monitors the namespace process
%% - every instance at each node (not only leaders) checks periodically
%%   that the 'base' namespace for the service
%%   is started, and belong to this service

-module(nkactor_master).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([register_namespace/2, get_namespace/2]).
-export([get_namespaces/1, get_namespaces/2, get_all_namespaces/1]).
-export([stop_all_namespaces/2]).
-export([srv_master_init/2, srv_master_handle_call/4,
         srv_master_handle_cast/3, srv_master_handle_info/3,
         srv_master_timed_check/3]).

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkACTOR Master (~s) "++Txt, [State#state.base_namespace|Args])).


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
    get_namespaces(SrvId, <<>>).

get_namespaces(SrvId, Namespace) ->
    case nkserver_master:call_leader(SrvId, {nkactor_get_namespaces, Namespace}) of
        {ok, NS} ->
            {ok, [{Name, Pid} || {_Parts, Name, Pid} <- NS]};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Finds all namespaces for all services under a base
get_all_namespaces(Namespace) ->
    Services = nkactor_util:get_services(),
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


-record(state, {
    base_namespace :: nkactor:namespace(),
    base_namespace_pid :: pid() | undefined,
    namespaces :: [{[binary()], nkactor:namespace(), pid()}]
}).

-type state() :: #{nkactor => #state{}}.


%% @private
-spec srv_master_init(nkserver:id(), state()) ->
    {continue, [{nkserver:id(), state()}]}.

srv_master_init(SrvId, State) ->
    BaseNamespace = nkserver:get_plugin_config(SrvId, nkactor, base_namespace),
    AcState = #state{
        base_namespace = BaseNamespace,
        namespaces = []
    },
    gen_server:cast(self(), nkactor_check_base_namespace),
    {continue, [SrvId, State#{nkactor => AcState}]}.


%% @private
srv_master_handle_call({nkactor_get_namespaces, <<>>}, _From, _SrvId, State) ->
    #state{namespaces = Namespaces} = get_actor_state(State),
    {reply, {ok, Namespaces}, State};

srv_master_handle_call({nkactor_get_namespaces, Namespace}, _From, _SrvId, State) ->
    #state{namespaces = NS1} = get_actor_state(State),
    Base = nkactor_lib:make_rev_parts(Namespace),
    NS2 = lists:filter(
        fun({Path, _N, _Pid}) -> lists:prefix(Base, Path) end,
        NS1),
    {reply, {ok, NS2}, State};

srv_master_handle_call({nkactor_register_namespace, Namespace, Pid}, _From, _SrvId, State) ->
    AcState = get_actor_state(State),
    case do_register_namespace(Namespace, Pid, AcState) of
        {ok, AcState2} ->
            {reply, {ok, self()}, set_actor_state(AcState2, State)};
        {error, Error} ->
            {reply, {error, Error}, State}
    end;

srv_master_handle_call({nkactor_get_namespace, Namespace}, _From, _SrvId, State) ->
    AcState = get_actor_state(State),
    Reply = do_find_namespace(Namespace, AcState),
    {reply, Reply, State};

srv_master_handle_call(_Msg, _From, _SrvId, _State) ->
    continue.


%% @private
srv_master_handle_cast(nkactor_check_base_namespace, SrvId, State) ->
    AcState1 = get_actor_state(State),
    AcState2 = check_base_namespace(SrvId, false, AcState1),
    {noreply, set_actor_state(AcState2, State)};

srv_master_handle_cast(_Msg,  _SrvId, _State) ->
    continue.


%% @private
srv_master_handle_info({'DOWN', _Ref, process, Pid, _Reason}, SrvId, State) ->
    AcState = get_actor_state(State),
    case AcState of
        #state{base_namespace_pid = Pid} ->
            ?LLOG(notice, "base namespace has failed!", [], AcState),
            AcState2 = AcState#state{base_namespace_pid = undefined},
            % We may not be leaders any more
            AcState3 = check_base_namespace(SrvId, false, AcState2),
            {noreply, set_actor_state(AcState3, State)};
        _ ->
            case do_remove_namespace(Pid, AcState) of
                {true, AcState2} ->
                    {noreply, set_actor_state(AcState2, State)};
                false ->
                    continue
            end
    end;

srv_master_handle_info(_Msg, _SrvId, _State) ->
    continue.


%% @private
srv_master_timed_check(IsLeader, SrvId, State) ->
    AcState1 = get_actor_state(State),
    AcState2 = check_base_namespace(SrvId, IsLeader, AcState1),
    {continue, [IsLeader, SrvId, set_actor_state(AcState2, State)]}.


%% ===================================================================
%% Register
%% ===================================================================

%% @private
%% All processes master (leader or not) check that base namespace is started
%% The leader also monitors it, to restart it immediately
check_base_namespace(SrvId, IsLeader, #state{base_namespace=Namespace}=AcState) ->
    case nkactor_namespace:get_pid(Namespace) of
        Pid when is_pid(Pid), IsLeader ->
            case AcState of
                #state{base_namespace_pid=Pid} ->
                    AcState;
                _ ->
                    monitor(process, Pid),
                    AcState#state{base_namespace_pid = Pid}
            end;
        Pid when is_pid(Pid) ->
            AcState;
        undefined ->
            spawn_link(fun() -> nkactor_namespace:do_start(SrvId, Namespace, true) end),
            AcState
    end.


%% @private
do_register_namespace(Namespace, Pid, AcState) ->
    #state{base_namespace=Base, namespaces=Namespaces} = AcState,
    case binary:match(Namespace, Base) of
        {S, T} when S+T == byte_size(Namespace) ->
            case lists:keyfind(Namespace, 2, Namespaces) of
                {_, _, Pid} ->
                    {ok, AcState};
                {Namespace, OldPid} ->
                    {error, {already_registered, OldPid}};
                false ->
                    monitor(process, Pid),
                    Parts = lists:reverse(binary:split(Namespace, <<".">>, [global])),
                    Namespaces2 = lists:sort([{Parts, Namespace, Pid}|Namespaces]),
                    AcState2 = AcState#state{namespaces=Namespaces2},
                    {ok, AcState2}
            end;
        _ ->
            ?LLOG(warning, "invalid namespace ~s for base ~s", [Namespace, Base], AcState),
            {error, service_invalid}
    end.


%% @private
do_find_namespace(Namespace, #state{namespaces=Namespaces}) ->
    case lists:keyfind(Namespace, 2, Namespaces) of
        {_, _, Pid} ->
            {true, Pid};
        false ->
            false
    end.


%% @private
do_remove_namespace(Pid, #state{namespaces=Namespaces}=AcState) ->
    case lists:keytake(Pid, 3, Namespaces) of
        {value, _, Namespaces2} ->
            {true, AcState#state{namespaces=Namespaces2}};
        false ->
            false
    end.


%% @private
get_actor_state(#{nkactor:=AcState}) -> AcState.

%% @private
set_actor_state(#state{}=AcState, State) -> State#{nkactor:=AcState}.