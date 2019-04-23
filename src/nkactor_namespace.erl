
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

%% @doc Namespace server
%% Namespaces are responsible for registering loaded actors and related counters
%%
%% Actor registration
%% ------------------
%%
%% When an actor starts, it registers with its namespace, calling register_actor/1
%% - The namespace is first located, we first try the local node cache
%% - If it is not found in cache or globally, it is started, first finding
%%   the service it belongs to, asking to base namespaces, until eventually
%%   reaching a 'base' namespace for a service, that will always be present
%%
%% When someone asks for a registered actor
%% - It is first tried in the local cache (valid for full name and UID)
%% - If not cached, and we have a full name, we use the namespace to ask for it
%%
%%
%% Namespace management
%% --------------------
%%
%% - Namespaces are started when the first actor registers with it
%% - The will stop after a timeout, if no actors remain registered
%%   (except for 'base' namespaces, that are started by the master service processes)
%% - If namespace stops normally, all registered actors will unload
%% - If it stops with an error, all registered actors will try to re-register with it
%%
%%
%% Future
%% ------
%%
%% - They save in disk periodically, first we check a saved ref of ourselves
%%   and update securely the saved time using this token
%% - If the save fails (someone saved in the middle) we stop
%% - If the save fails, but the stored one is too old, we overwrite it
%% - If a service is started using a previous namespace as its new 'base', all namespaces
%%   over it should be killed. Actors will re-register, namespaces will be restarted
%%   using the new service, and actors will also take the new service
%%
%% - Namespace must check in disk if they are already started at another cluster
%% - If it is started at another cluster, a shadow one is started connected to the
%%   remote cluster. If the link fails, the shadow namespace stops. Next try to
%%   start it may now use the secondary data center, once the settled time on disk passes
%%
%%
%%
%%
%%
%%
%%






-module(nkactor_namespace).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([register_actor/1, find_actor/1, find_registered_actor/1, get_registered_actors/1]).
-export([get_counters/1, get_detailed_counters/1]).
-export([get_namespace/1, find_namespace/1, stop_namespace/2]).
-export([register_index/2, find_index/2]).
-export([get_pid/1, do_start/3]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2,
         handle_info/2]).

-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").

-define(LLOG(Type, Txt, Args, State),
    lager:Type("NkACTOR Namespace '~s' (~s) "++Txt, [State#state.namespace, State#state.id | Args])).

-define(TIMEOUT, 5000).



%% ===================================================================
%% Public - Actor registration
%% ===================================================================


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
    true = is_binary(Group) andalso is_binary(Res) andalso is_binary(Name) andalso
        is_binary(Namespace) andalso is_binary(UID) andalso is_pid(Pid),
    case start_and_call(Namespace, {nkactor_register_actor, ActorId}) of
        {ok, SrvId, NamespacePid} ->
            % This actor will probably we used now in this node
            store_actor_in_cache(SrvId, ActorId),
            {ok, SrvId, NamespacePid};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Checks if an actor is activated
%% - checks if it is in the local  (full info or UID)
%% - if not, asks to the namespace, only if we have full info (group, resource, name and namespace)

-spec find_actor(nkactor:id()) ->
    {true, nkserver:id(), #actor_id{}} | {false, nkserver:id()} | false.

find_actor(Id) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case find_cached_actor(ActorId) of
        {true, SrvId, ActorId2} ->
            {true, SrvId, ActorId2};
        false ->
            find_registered_actor(ActorId)
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
                {error, {namespace_not_found, _}} ->
                    false;
                {error, Error} ->
                    {error, Error}
            end;
        false ->
            false
    end.


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


%% @private
get_registered_actors(Namespace) ->
    start_and_call(Namespace, nkactor_get_registered_actors).


%% @doc
get_counters(Namespace) ->
    lists:foldl(
        fun({_Path, _Srv, Pid}, Acc) ->
            case catch gen_server:call(Pid, nkactor_get_counters) of
                {ok, Counters} ->
                    maps:fold(
                        fun(Type, Value, Acc2) ->
                            Count = maps:get(Type, Acc2, 0),
                            Acc2#{Type => Count+Value}
                        end,
                        Acc,
                        Counters);
                _ ->
                    Acc
            end
        end,
        #{},
        nkactor_master:get_all_namespaces(Namespace)).


%% @doc
get_detailed_counters(Namespace) ->
    lists:foldl(
        fun({_Path, _Srv, Pid}, Acc) ->
            case catch gen_server:call(Pid, nkactor_get_counters) of
                {ok, Counters} ->
                    Acc#{_Path => Counters};
                _ ->
                    Acc
            end
        end,
        #{},
        nkactor_master:get_all_namespaces(Namespace)).


%% @doc
-spec register_index(#actor_id{}, term()) ->
    ok | {error, index_already_defined|actor_not_registered|term()}.

register_index(#actor_id{namespace=Namespace}=ActorId, Index) ->
    call(Namespace, {nkactor_register_index, ActorId, Index}).


-spec find_index(nkserver:namespace(), term()) ->
    {true, #actor_id{}} | false | {error, term()}.

find_index(Namespace, Index) ->
    call(Namespace, {nkactor_find_index, Index}).



%% ===================================================================
%% Public - Namespace management
%% ===================================================================


%% @doc Finds an existing namespace or starts a new one, storing it in local cache
-spec get_namespace(nkactor:namespace()) ->
    {ok, nkserver:id(), pid()} | {error, nkserver:msg()}.

get_namespace(Namespace) ->
    Namespace2 = to_bin(Namespace),
    case nklib_proc:values({nkactor_namespace, Namespace2}) of
        [{SrvId, Pid}|_] ->
            {ok, SrvId, Pid};
        [] ->
            case start_and_call(Namespace2, nkactor_get_namespace) of
                {ok, SrvId, Pid} ->
                    nklib_proc:put({nkactor_namespace, Namespace2}, SrvId, Pid),
                    {ok, SrvId, Pid};
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc
start_namespace(Namespace) ->
    Namespace2 = to_bin(Namespace),
    case find_namespace(Namespace2) of
        Pid when is_pid(Pid) ->
            {ok, Pid};
        undefined ->
            case find_service(Namespace2) of
                {ok, SrvId} ->
                    do_start(SrvId, Namespace2, false);
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc
find_namespace(Namespace) ->
    Namespace2 = to_bin(Namespace),
    case nklib_proc:values({nkactor_namespace, Namespace2}) of
        [{_SrvId, Pid}|_] ->
            Pid;
        [] ->
            case get_pid(Namespace2) of
                Pid when is_pid(Pid) ->
                    Pid;
                undefined ->
                    undefined
            end
    end.


%% @doc Stops a running namespace
%% If reason is 'normal' all registered actors will stop, and they will not restart automatically
%% For other reasons (or if it is killed) actors will re-register
stop_namespace(Namespace, Reason) ->
    Namespace2 = to_bin(Namespace),
    case find_namespace(Namespace2) of
        Pid when is_pid(Pid) ->
            gen_server:cast(Pid, {nkactor_stop, Reason});
        undefined ->
            {error, namespace_not_started}
    end.


%% @doc Tries to find the service that manages a namespace
%% If the called namespace does not exists, the "parent" namespace is tried
-spec find_service(nkactor:namespace()) ->
    {ok, nkserver:id()} | {error, term()}.

find_service(Namespace) ->
    case call(Namespace, nkactor_get_namespace) of
        {ok, SrvId, _Pid} ->
            {ok, SrvId};
        {error, _} ->
            case binary:split(Namespace, <<".">>) of
                [Base] ->
                    % Namespace has no "." in it, is a base one
                    case call(Base, nkactor_get_namespace) of
                        {ok, SrvId, _Pid} ->
                            {ok, SrvId};
                        {error, Error} ->
                            {error, Error}
                    end;
                [_Head, Base] ->
                    find_service(Base)
            end
    end.


%% @private Starts a new namespace and registers it globally
%% It will normally stop after a timeout if no actor is registered,
%% unless IsMain is true
-spec do_start(nkserver:id(), nkactor:namespace(), boolean()) ->
    {ok, pid()} | {error, term()}.

do_start(SrvId, Namespace, IsMain) when is_binary(Namespace)->
    case nkactor_lib:normalized_name(Namespace) of
        Namespace ->
            Global = get_global_name(Namespace),
            gen_server:start({global, Global}, ?MODULE, [SrvId, Namespace, IsMain], []);
        _ ->
            {error, {namespace_invalid, Namespace}}
    end.


%% @private
call(Namespace, Msg) ->
    case nklib_util:call2({global, get_global_name(Namespace)}, Msg) of
        process_not_found ->
            {error, {namespace_not_found, Namespace}};
        Other ->
            Other
    end.


%% @private
start_and_call(Namespace, Msg) ->
    start_and_call(Namespace, Msg, 5000, 3).


%% @private
start_and_call(Namespace, Msg, Timeout, Tries) when Tries > 0 ->
    case start_namespace(Namespace) of
        {ok, Pid} ->
            case nklib_util:call2(Pid, Msg, Timeout) of
                process_not_found ->
                    timer:sleep(250),
                    lager:warning("NkACTOR Namespace '~s' failed, retrying...", [Namespace]),
                    start_and_call(Namespace, Msg, Timeout, Tries-1);
                Other ->
                    Other
            end;
        {error, Error} ->
            {error, Error}
    end;

start_and_call(Namespace, _Msg, _Timeout, _Tries) ->
    lager:error("NkACTOR could no start namespace '~s'", [Namespace]),
    {error, {namespace_not_found, Namespace}}.


%% @private
get_pid(Namespace) ->
    global:whereis_name(get_global_name(Namespace)).


%% @private
get_global_name(Namespace) ->
    {nkactor_namespace, to_bin(Namespace)}.


%% ===================================================================
%% gen_server
%% ===================================================================

% Stores counters for registered actors
-type counters() :: #{
    nkactor:group() => #{
        nkactor:resource() => #{nkactor:id()|<<>> => integer()}}}.


-record(state, {
    id :: nkserver:id(),
    namespace :: nkactor:namespace(),
    is_master :: boolean(),
    master_pid :: pid() | undefined,
    register_ets :: term(),
    counters = #{} :: counters()
}).


%% @private
init([SrvId, Namespace, IsMaster]) ->
    case nkactor_master:register_namespace(SrvId, Namespace) of
        {ok, MasterPid} ->
            monitor(process, MasterPid),
            State = #state{
                id = SrvId,
                is_master = IsMaster,
                namespace = Namespace,
                master_pid = MasterPid,
                register_ets = ets:new(nkactor_register, [])
            },
            ?LLOG(notice, "started (~p)", [self()], State),
            case IsMaster of
                true ->
                    {ok, State};
                false ->
                    {ok, State, ?TIMEOUT}
            end;
        {error, Error} ->
            {stop, Error}
    end.


%% @private
-spec handle_call(term(), {pid(), term()}, #state{}) ->
    {noreply, #state{}} | {reply, term(), #state{}} |
    {stop, Reason::term(), #state{}} | {stop, Reason::term(), Reply::term(), #state{}}.

handle_call(nkactor_get_namespace, _From, #state{id=SrvId}=State) ->
    reply({ok, SrvId, self()}, State);

handle_call({nkactor_register_actor, ActorId}, _From, State) ->
    #state{id=SrvId, namespace = Namespace} = State,
    case ActorId of
        #actor_id{namespace = Namespace} ->
            case do_register_actor(ActorId, State) of
                {ok, State2} ->
                    reply({ok, SrvId, self()}, State2);
                {error, Error} ->
                    reply({error, Error}, State)
            end;
        _ ->
            reply({error, actor_namespace_invalid}, State)
    end;

handle_call({nkactor_find_actor, #actor_id{}=ActorId}, _From, #state{id=SrvId}=State) ->
    Reply = case do_find_actor_name(ActorId, State) of
        {true, ActorId2, _Indices} ->
            {true, SrvId, ActorId2};
        false ->
            {false, SrvId}
    end,
    reply(Reply, State);

handle_call(nkactor_get_registered_actors, _From, #state{register_ets=Ets}=State) ->
    List = [ActorId || {{name, _Group, _Res, _Name}, ActorId, _Idx} <- ets:tab2list(Ets)],
    reply(List, State);

handle_call(nkactor_get_counters, _From, #state{counters=Counters}=State) ->
    reply({ok, Counters}, State);

handle_call({nkactor_register_index, ActorId, Index}, _From, State) ->
    Reply = do_register_index(ActorId, Index, State),
    {reply, Reply, State};

handle_call({nkactor_find_index, Index}, _From, State) ->
    Reply = do_find_index(Index, State),
    reply(Reply, State);

handle_call(Msg, _From, State) ->
    lager:warning("Module ~p received unexpected call: ~p", [?MODULE, Msg]),
    noreply(State).


%% @private
-spec handle_cast(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_cast({nkactor_stop, Reason}, State) ->
    {stop, Reason, State};

handle_cast(Msg, State) ->
    lager:warning("Module ~p received unexpected cast: ~p", [?MODULE, Msg]),
    noreply(State).


%% @private
-spec handle_info(term(), #state{}) ->
    {noreply, #state{}} | {stop, term(), #state{}}.

handle_info(timeout, #state{counters=Counters}=State) ->
    case map_size(Counters) of
        0 ->
            ?LLOG(notice, "has no childs, stopping", [], State),
            {stop, normal, State};
        _ ->
            % ?LLOG(info, "has childs, not stopping", [], State),
            noreply(State)
    end;

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #state{master_pid=Pid}=State) ->
    ?LLOG(warning, "master leader has failed (~p)", [Pid], State),
    {stop, normal, State};

handle_info({'DOWN', _Ref, process, Pid, _Reason}, State) ->
    case do_remove_actor(Pid, State) of
        {true, State2} ->
            noreply(State2);
        false ->
            lager:warning("Module ~p received unexpected down: ~p", [?MODULE, Pid]),
            noreply(State)
    end;

handle_info(Msg, State) ->
    lager:warning("Module ~p received unexpected info: ~p", [?MODULE, Msg]),
    noreply(State).


%% @private
-spec code_change(term(), #state{}, term()) ->
    {ok, #state{}}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.


%% @private
-spec terminate(term(), #state{}) ->
    ok.

%%terminate(Reason, State) ->
%%    case Reason of
%%        normal ->
%%            stop_all_childs(State);
%%        _ ->
%%            ok
%%    end.

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
                {{name, Group, Res, Name}, ActorId, []},
                {{pid, Pid}, {name, Group, Res, Name}, UID, Ref}
            ],
            ets:insert(Ets, Objs),
            State2 = do_add_actor(ActorId, State),
            {ok, State2};
        {true, _OldActorId, _Indices} ->
            {error, actor_already_registered}
    end.


%% @private
do_find_actor_name(#actor_id{}=ActorId, #state{register_ets=Ets}) ->
    #actor_id{group=Group, resource=Res, name=Name} = ActorId,
    case ets:lookup(Ets, {name, Group, Res, Name}) of
        [{_, ActorId2, Indices}] ->
            {true, ActorId2, Indices};
        [] ->
            false
    end.


%% @private
do_remove_actor(Pid, #state{register_ets=Ets}=State) ->
    case ets:lookup(Ets, {pid, Pid}) of
        [{{pid, Pid}, {name, Group, Res, Name}, UID, Ref}] ->
            nklib_util:demonitor(Ref),
            [{_, _, Indices}] = ets:lookup(Ets, {name, Group, Res, Name}),
            ets:delete(Ets, {pid, Pid}),
            ets:delete(Ets, {name, Group, Res, Name}),
            lists:foreach(fun(Index) -> ets:delete(Ets, {index, Index}) end, Indices),
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


%% @private
do_register_index(ActorId, Index, #state{register_ets=Ets}=State) ->
    case do_find_actor_name(ActorId, State) of
        {true, ActorId2, Indices} ->
            case do_find_index(Index, State) of
                false ->
                    #actor_id{group=Group, resource=Res, name=Name} = ActorId2,
                    Objs = [
                        {{name, Group, Res, Name}, ActorId, [Index|Indices]},
                        {{index, Index}, ActorId2}
                    ],
                    ets:insert(Ets, Objs),
                    ok;
                {true, _} ->
                    {error, index_already_defined}
            end;
        false ->
            {error, actor_not_registered}
    end.


%% @private
do_find_index(Index, #state{register_ets=Ets}) ->
    case ets:lookup(Ets, {index, Index}) of
        [{_, ActorId}] ->
            {true, ActorId};
        [] ->
            false
    end.


%% ===================================================================
%% Counters
%% ===================================================================

%% @private
do_add_actor(#actor_id{group=Group, resource=Res}, #state{counters=Types}=State) ->
    Type = <<Group/binary, $:, Res/binary>>,
    Number = maps:get(Type, Types, 0),
    Types2 = Types#{Type => Number+1},
    State#state{counters = Types2}.


%% @private
do_rm_actor_counters(#actor_id{group=Group, resource=Res}, #state{counters=Types}=State) ->
    Type = <<Group/binary, $:, Res/binary>>,
    Number = maps:get(Type, Types, 0),
    Types2 = case Number-1 of
        0 ->
            maps:remove(Type, Types);
        Number2 when Number2 > 0 ->
            Types#{Type => Number2}
    end,
    State#state{counters = Types2}.


%% ===================================================================
%% Other
%% ===================================================================

%% @private
reply(Reply, #state{is_master=true}=State) ->
    {reply, Reply, State};

reply(Reply, #state{is_master=false}=State) ->
    {reply, Reply, State, ?TIMEOUT}.


%% @private
noreply(#state{is_master=true}=State) ->
    {noreply, State};

noreply(#state{is_master=false}=State) ->
    {noreply, State, ?TIMEOUT}.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).


%%%% @private
%%stop_all_childs(#state{register_ets=Ets}) ->
%%    Pids = [Pid || {{pid, Pid}, _, _, _} <- ets:tab2list(Ets)],
%%    lists:foreach(
%%        fun(Pid) -> nkactor_srv:async_op(Pid, {raw_stop, namespace_stop}) end,
%%        Pids).
