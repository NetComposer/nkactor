
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

%% @doc Started after nkactor_srv on each node, one of them is elected 'leader'
%% - We register with our node process to get updated status of all available nodes,
%%   each time an update is received we check the nodes where we must start or
%%   stop the service
%% - Each service instance sends us periodically full status, each time we check if
%%   it is running the our same version, or update it if not
%%   We also perform registrations for actors.
%% - A master is elected, and re-checked periodically
%% - If we die, a new leader is elected, actors will register again

-module(nkactor_namespace).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([get_all/0]).
-export([find_registered/1, find_namespace/1]).
-export([register_actor/1, get_registered_actors/1]).
-export([find_service/1, start/1, start/2, get_pid/1, call/2]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkACTOR Namespace (~s, ~s) "++Txt, [State#state.namespace, State#state.id | Args])).

-define(POOL_MAX_CONNECTIONS, 10).
-define(POOL_TIMEOUT, 30*60*1000).



%% ==================================================================
%% Types
%% ===================================================================


% Stores all registered information by class and resource
-type class_types() :: #{
    nkactor:group() => #{
        nkactor:resource() => #{
            nkactor:name() => nkactor:uid()}}}.

% Stores counters for registered actors
-type counters() :: #{
    nkactor:group() => #{
        nkactor:resource() => #{nkactor:id()|<<>> => integer()}}}.





%% ===================================================================
%% Public
%% ===================================================================

%% @doc
get_all() ->
    Services = nkactor_util:get_services(),
    NS1 = lists:foldl(
        fun(SrvId, Acc) ->
            case nkactor_master:get_namespaces(SrvId) of
                {ok, N1} ->
                    N2 = [{Path, N, SrvId, Pid} || {Path, N, Pid} <-N1],
                    [N2|Acc];
                {error, _Error} ->
                    Acc
            end
        end,
        [],
        Services),
    NS2 = lists:sort(lists:flatten(NS1)),
    [{N, SrvId, Pid} || {_Path, N, SrvId, Pid} <- NS2].



%% @doc Checks if an actor is activated
%% - checks if it is in the local  (full info or UID)
%% - if not, asks to the namespace, only if we have full info (group, resource, name and namespace)

-spec find_registered(nkactor:id()) ->
    {true, nkserver:id(), #actor_id{}} | {false, nkserver:id()} | false.

find_registered(Id) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case find_cached_actor(ActorId) of
        {true, SrvId, ActorId2} ->
            {true, SrvId, ActorId2};
        false ->
            find_registered_actor(ActorId)
    end.


%% @doc
-spec find_namespace(nkactor:namespace()) ->
    {ok, nkserver:id(), pid()} | {error, nkserver:msg()}.

find_namespace(Namespace) ->
    case nklib_proc:values({nkactor_namespace, Namespace}) of
        [{SrvId, Pid}|_] ->
            {ok, SrvId, Pid};
        [] ->
            case start(Namespace) of
                {ok, Pid} ->
                    {ok, SrvId} = gen_server:call(Pid, nkactor_get_service),
                    nklib_proc:put({nkactor_namespace, Namespace}, SrvId, Pid),
                    {ok, SrvId, Pid};
                {error, Error} ->
                    {error, Error}
            end
    end.



%% @doc Register an actor with the namespace server
%% If successful, stores in local cache
-spec register_actor(#actor_id{}) ->
    {ok, nkserver:id(), pid()} | {error, nkserver:msg()}.

register_actor(#actor_id{uid=UID, pid=Pid, namespace=Namespace}=ActorId) ->
    #actor_id{
        group = Group,
        resource = Res,
        name = Name,
        namespace = Namespace,
        uid = UID,
        pid = Pid
    } = ActorId,
    case
        is_binary(Group) andalso is_binary(Res) andalso is_binary(Name) andalso
        is_binary(Namespace) andalso is_binary(UID) andalso is_pid(Pid)
    of
        true ->
            case start_and_call(Namespace, {nkactor_register_actor, ActorId}) of
                {ok, SrvId, NamespacePid} ->
                    store_actor_in_cache(SrvId, ActorId),
                    {ok, SrvId, NamespacePid};
                {error, Error} ->
                    {error, Error}
            end;
        false ->
            {error, actor_id_invalid}
    end.



%% @doc Finds an actor loaded and registered with a namespace
%% If successful, stores in local cache
-spec find_registered_actor(#actor_id{}) ->
    {true, nkserver:id(), #actor_id{}}| {false, nkserver:id()} | false | {error, nkserver:msg()}.

find_registered_actor(#actor_id{namespace=Namespace}=ActorId) ->
    #actor_id{
        group = Group,
        resource = Res,
        name = Name,
        namespace = Namespace
    } = ActorId,
    case
        is_binary(Group) andalso is_binary(Res) andalso
        is_binary(Name) andalso is_binary(Namespace)
    of
        true ->
            case call(Namespace, {nkactor_find_actor, ActorId}) of
                {true, SrvId, ActorId2} ->
                    store_actor_in_cache(SrvId, ActorId2),
                    {true, SrvId, ActorId2};
                {false, SrvId} ->
                    {false, SrvId};
                {error, Error} ->
                    {error, Error}
            end;
        false ->
            false
    end.


%% @private
get_registered_actors(Namespace) ->
    call(Namespace, nkactor_get_registered_actors).





%% ===================================================================
%% Private
%% ===================================================================


%% @private
start_and_call(Namespace, Msg) ->
    case start(Namespace) of
        {ok, Pid} ->
            gen_server:call(Pid, Msg);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
start(Namespace) ->
    case get_pid(Namespace) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        undefined ->
            case find_service(Namespace) of
                {ok, SrvId} ->
                    start(SrvId, Namespace);
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc
find_service(Namespace) ->
    case call(Namespace, nkactor_get_service) of
        {ok, SrvId} ->
            {ok, SrvId};
        {error, namespace_not_found} ->
            case binary:split(Namespace, <<".">>) of
                [Base] ->
                    % Namespace has no "." in it, is a base one
                    case call(Base, nkactor_get_service) of
                        {ok, SrvId} ->
                            {ok, SrvId};
                        {error, namespace_not_found} ->
                            {error, namespace_not_found}
                    end;
                [_Head, Base] ->
                    find_service(Base)
            end
    end.


%% @private
call(Namespace, Msg) ->
    call(Namespace, Msg, 5000).


%% @private
call(Namespace, Msg, Timeout) ->
    case get_pid(Namespace) of
        Pid when is_pid(Pid) ->
            gen_server:call(Pid, Msg, Timeout);
        undefined ->
            {error, namespace_not_found}
    end.


%% @private
-spec start(nkserver:id(), nkactor:namespace()) ->
    {ok, pid()} | {error, term()}.

start(SrvId, Namespace) when is_binary(Namespace)->
    Global = get_global_name(Namespace),
    gen_server:start({global, Global}, ?MODULE, [SrvId, Namespace], []).


%% @private
get_pid(Namespace) ->
    global:whereis_name(get_global_name(Namespace)).


%% @private
get_global_name(Namespace) ->
    {nkactor_namespace, Namespace}.


%% @private
%% Find if the actor id is cached in this node, by name or UID
find_cached_actor(#actor_id{name=undefined, uid=UID}) when is_binary(UID) ->
    case nklib_proc:values({nkactor_uid, UID}) of
        [{{SrvId, ActorId2}, _Pid}|_] ->
            {true, SrvId, ActorId2};
        [] ->
            false
    end;

find_cached_actor(#actor_id{name=Name}=ActorId) when is_binary(Name) ->
    #actor_id{group=Group, resource=Res, namespace=Namespace} = ActorId,
    case nklib_proc:values({nkactor, Group, Res, Name, Namespace}) of
        [{{SrvId, ActorId2}, _Pid}|_] ->
            {true, SrvId, ActorId2};
        [] ->
            false
    end.


%% @private
store_actor_in_cache(SrvId, ActorId) ->
    #actor_id{
        group = Group,
        resource = Res,
        name = Name,
        namespace = Namespace,
        uid = UID,
        pid = Pid
    } = ActorId,
    true = is_pid(Pid),
    nklib_proc:put({nkactor, Group, Res, Name, Namespace}, {SrvId, ActorId}, Pid),
    nklib_proc:put({nkactor_uid, UID}, {SrvId, ActorId}, Pid).




%% ===================================================================
%% gen_server
%% ===================================================================


-record(state, {
    id :: nkserver:id(),
    namespace :: nkactor:namespace(),
    master_pid :: pid() | undefined,
    register_ets :: term(),
    actor_uids = #{} :: #{nkactor:id() => boolean()},
    actor_group_types = #{} :: class_types(),
    counters = #{} :: counters()
}).



%% @private
init([SrvId, Namespace]) ->
    case nkactor_master:register_namespace(SrvId, Namespace) of
        {ok, MasterPid} ->
            monitor(process, MasterPid),
            State = #state{
                id = SrvId,
                namespace = Namespace,
                master_pid = MasterPid,
                register_ets = ets:new(nkactor_register, [])
            },
            {ok, State};
        {error, Error} ->
            {stop, Error}
    end.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} |
    {stop, Reason::term(), #state{}} | {stop, Reason::term(), Reply::term(), #state{}}.

handle_call(nkactor_get_service, _From, #state{id=SrvId}=State) ->
    {reply, {ok, SrvId}, State};

handle_call({nkactor_register_actor, ActorId}, _From, State) ->
    #state{id=SrvId, namespace = Namespace} = State,
    case ActorId of
        #actor_id{namespace = Namespace} ->
            case do_register_actor(ActorId, State) of
                {ok, State2} ->
                    {reply, {ok, SrvId, self()}, State2};
                {error, Error} ->
                    {reply, {error, Error}, State}
            end;
        _ ->
            {reply, {error, actor_invalid}, State}
    end;

handle_call({nkactor_find_actor, #actor_id{}=ActorId}, _From, #state{id=SrvId}=State) ->
    Reply = case do_find_actor_name(ActorId, State) of
        {true, ActorId2} ->
            {true, SrvId, ActorId2};
        false ->
            {false, SrvId}
    end,
    {reply, Reply, State};

handle_call(nkactor_get_registered_actors, _From, #state{register_ets=Ets}=State) ->
    List = [ActorId || {{uid, _}, ActorId} <- ets:tab2list(Ets)],
    {reply, List, State};

handle_call(Msg, _From, State) ->
    lager:warning("Module ~p received unexpected call: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_cast(Msg, State) ->
    lager:warning("Module ~p received unexpected cast: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{master_pid=Pid}=State) ->
    ?LLOG(warning, "master has failed (~p)", [Pid], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    case do_remove_actor(Pid, State) of
        {true, State2} ->
            {noreply, State2};
        false ->
            lager:warning("Module ~p received unexpected down: ~p", [?MODULE, Pid]),
            {noreply, State}
    end;

handle_info(Msg, State) ->
    lager:warning("Module ~p received unexpected info: ~p", [?MODULE, Msg]),
    {noreply, State}.


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

terminate(_Reason, _State) ->
    ok.



%% ===================================================================
%% Register & Counters
%% ===================================================================

do_register_actor(ActorId, #state{register_ets=Ets}=State) ->
    case do_find_actor_name(ActorId, State) of
        false ->
            #actor_id{group=Group, resource=Res, name=Name, uid=UID, pid=Pid} = ActorId,
            Ref = monitor(process, Pid),
            Objs = [
                {{name, Group, Res, Name}, ActorId},
                {{pid, Pid}, {name, Group, Res, Name}, UID, Ref}
            ],
            ets:insert(Ets, Objs),
            State2 = do_rm_actor_counters(ActorId, State),
            State3 = do_add_actor_counters(ActorId, State2),
            {ok, State3};
        {true, _OldActorId} ->
            {error, actor_already_registered}
    end.


%% @private
do_find_actor_name(#actor_id{}=ActorId, #state{register_ets=Ets}) ->
    #actor_id{group=Group, resource=Res, name=Name} = ActorId,
    case ets:lookup(Ets, {name, Group, Res, Name}) of
        [{_, ActorId2}] ->
            {true, ActorId2};
        [] ->
            false
    end.


%% @private
do_remove_actor(Pid, #state{register_ets=Ets}=State) ->
    case ets:lookup(Ets, {pid, Pid}) of
        [{{pid, Pid}, {name, Group, Res, Name}, UID, Ref}] ->
            nklib_util:demonitor(Ref),
            ets:delete(Ets, {pid, Pid}),
            ets:delete(Ets, {name, Group, Res, Name}),
            ActorId = #actor_id{
                uid = UID,
                group = Group,
                resource = Res,
                name = Name
            },
            State2 = do_rm_actor_counters(ActorId, State),
            {true, State2};
        [] ->
            false
    end.


%% ===================================================================
%% Counters
%% ===================================================================

%% @private
do_add_actor_counters(ActorId, State) ->
    #actor_id{uid=UID, group=Group, resource=Res, name=Name} = ActorId,
    #state{actor_uids=UIDs, actor_group_types=GroupResources, counters=Counters} = State,
    Resources1 = maps:get(Group, GroupResources, #{}),
    Names1 = maps:get(Res, Resources1, #{}),
    Names2 = Names1#{Name => UID},
    Resources2 = Resources1#{Res => Names2},
    GroupResources2 = GroupResources#{Group => Resources2},
    Counters2 = case maps:is_key(UID, UIDs) of
        false ->
            GroupCounters1 = maps:get(Group, Counters, #{}),
            ResCounter1 = maps:get(Res, GroupCounters1, 0),
            GroupCounters2 = GroupCounters1#{Res => ResCounter1+1},
            Counters#{Group => GroupCounters2};
        true ->
            Counters
    end,
    State#state{
        actor_uids = UIDs#{UID => true},
        actor_group_types = GroupResources2,
        counters = Counters2
    }.


%% @private
do_rm_actor_counters(ActorId, State) ->
    #actor_id{uid=UID, group=Group, resource=Res, name=Name} = ActorId,
    #state{actor_uids=UIDs, actor_group_types=GroupResources, counters=Counters} = State,
    Resources1 = maps:get(Group, GroupResources, #{}),
    Names1 = maps:get(Res, Resources1, #{}),
    Names2 = maps:remove(Name, Names1),
    Resources2 = case map_size(Names2) of
        0 ->
            maps:remove(Res, Resources1);
        _ ->
            Resources1#{Res => Names2}
    end,
    GroupResources2 = GroupResources#{Group => Resources2},
    Counters2 = case maps:is_key(UID, UIDs) of
        true ->
            GroupCounters1 = maps:get(Group, Counters),
            ResCounter1 = maps:get(Res, GroupCounters1),
            GroupCounters2 = case ResCounter1-1 of
                0 ->
                    maps:remove(Res, GroupCounters1);
                ResCounter2 ->
                    GroupCounters1#{Res => ResCounter2}
            end,
            Counters#{Group => GroupCounters2};
        false ->
            Counters
    end,
    State#state{
        actor_uids = maps:remove(UID, UIDs),
        actor_group_types = GroupResources2,
        counters = Counters2
    }.


%% ===================================================================
%% Other
%% ===================================================================


%%%% @private
%%set_http_pool(#state{actor=Actor}) ->
%%    #actor{id=#actor_id{domain=Domain}, data=Data} = Actor,
%%    Spec = maps:get(spec, Data, #{}),
%%    Pool = maps:get(<<"httpPool">>, Spec, #{}),
%%    Max = maps:get(<<"maxConnections">>, Pool, ?POOL_MAX_CONNECTIONS),
%%    Timeout = maps:get(<<"timeout">>, Pool, ?POOL_TIMEOUT),
%%    ok = hackney_pool:start_pool(Domain, []),
%%    ok = hackney_pool:set_max_connections(Domain, Max),
%%    ok = hackney_pool:set_timeout(Domain, Timeout),
%%    Max = hackney_pool:max_connections(Domain),
%%    Timeout = hackney_pool:timeout(Domain),
%%    ?ACTOR_LOG(notice, "started Hackney Pool ~s (~p, ~p)", [Domain, Max, Timeout]),
%%    ok.
%%
%%
%%%% @private
%%stop_http_pool(#actor_st{actor=Actor}) ->
%%    #actor{id=#actor_id{domain=Domain}} = Actor,
%%    hackney_pool:stop_pool(Domain).



%%%% @private
%%do_stop_all_actors(#state{register_ets=Ets}=State) ->
%%    ets:foldl(
%%        fun
%%            ({{pid, Pid}, {name, _Domain, _Group, _Res, _Name}, _UID, _Ref}, Acc) ->
%%                nkactor_srv:async_op(none, Pid, {raw_stop, father_stopped}),
%%                Acc+1;
%%            (_Term, Acc) ->
%%                Acc
%%        end,
%%        0,
%%        Ets).


