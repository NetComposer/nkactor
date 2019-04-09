%% -------------------------------------------------------------------
%%
%% srvCopyright (c) 2019 Carlos Gonzalez Florido.  All Rights Reserved.
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

%% @doc Actor DB-related module
-module(nkactor_backend).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([find/1, activate/2, read/2]).
-export([create/2, update/3, delete/2, delete_multi/2]).
-export([search/3, aggregation/3]).
%% -export([check_service/4]).
%%-export_type([search_obj/0, search_objs_opts/0]).

-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").


-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR DB "++Txt, Args)).

%% ===================================================================
%% Types
%% ===================================================================



-type search_type() :: term().

-type agg_type() :: term().

-type search_obj() :: #{binary() => term()}.




%% ===================================================================
%% Public
%% ===================================================================


%% @doc Finds and actor from UUID or Path, in memory or disk, and checks activation
%%
%% For Ids having a namespace:
%% - If it is currently activated, it will be found, and full data will be returned,
%%   along with pid
%% - If it is not activated, but we have a persistence module, we will load it from disk
%%
%% For Ids not having a namespace
%% - If it is currently activated, and cached locally, it will be found with full data
%% - If not, and we have a persistence module, it will be loaded from disk
%% - We then check if it is activated, once we have the namespace
%%
-spec find(nkactor:id()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

find(Id) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case nkactor_namespace:find_actor(ActorId) of
        {true, SrvId, #actor_id{pid=Pid}=ActorId2} when is_pid(Pid) ->
            % It is registered or cached
            {ok, SrvId, ActorId2, #{}};
        {false, SrvId} ->
            do_find([SrvId], ActorId);
        false ->
            SrvIds = nkactor_util:get_services(),
            do_find(SrvIds, ActorId)
    end.


%% @private
do_find([], _ActorId) ->
    {error, actor_not_found};

do_find([SrvId|Rest], ActorId) ->
    case ?CALL_SRV(SrvId, actor_db_find, [SrvId, ActorId, #{}]) of
        {ok, #actor_id{} = ActorId2, Meta} ->
            % If its was an UID, or a partial path, we must check now that
            % it could be registered, now that we have full data
            case nkactor_namespace:find_actor(ActorId2) of
                {true, _SrvId, #actor_id{pid = Pid} = ActorId3} when is_pid(Pid) ->
                    {ok, SrvId, ActorId3, Meta};
                _ ->
                    {ok, SrvId, ActorId2, Meta}
            end;
        {error, actor_not_found} ->
            do_find(Rest, ActorId);
        {error, Error} ->
            {error, Error}
    end.



%% @doc Finds an actors's pid or loads it from storage and activates it
-spec activate(nkactor:id(), #{ttl=>integer()}) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

activate(Id, Opts) ->
    case find(Id) of
        {ok, SrvId, #actor_id{pid=Pid}=ActorId, Meta} when is_pid(Pid) ->
            {ok, SrvId, ActorId, Meta};
        {ok, SrvId, ActorId, _Meta} ->
            case do_read(SrvId, ActorId, Opts) of
                {ok, Actor, Meta2} ->
                    Config = case Opts of
                        #{ttl:=TTL} ->
                            #{ttl => TTL};
                        _ ->
                            #{}
                    end,
                    case
                        ?CALL_SRV(SrvId, actor_activate, [Actor, Config])
                    of
                        {ok, ActorId3} ->
                            {ok, SrvId, ActorId3, Meta2};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, persistence_not_defined} ->
                    {error, actor_not_found};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @doc Reads an actor from memory if loaded, or disk if not
%% It will first try to activate it (unless indicated)
%% If consume is set, it will destroy the object on read
%% SrvId is used for calling the DB
-spec read(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkactor:actor(),  Meta::map()} | {error, actor_not_found|term()}.

read(Id, #{activate:=false}=Opts) ->
    case maps:get(consume, Opts, false) of
        true ->
            {error, cannot_consume};
        false ->
            case find(Id) of
                {ok, SrvId, ActorId, _} ->
                    case do_read(SrvId, ActorId, Opts) of
                        {ok, Actor, DbMeta} ->
                            {ok, SrvId, Actor, DbMeta};
                        {error, persistence_not_defined} ->
                            {error, actor_not_found};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end
    end;

read(Id, Opts) ->
    case activate(Id, Opts) of
        {ok, SrvId, ActorId, Meta} ->
            Consume = maps:get(consume, Opts, false),
            Op = case Consume of
                true ->
                    consume_actor;
                false ->
                    get_actor
            end,
            case nkactor_srv:sync_op(ActorId, Op, infinity) of
                {ok, Actor} ->
                    {ok, SrvId, Actor, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @doc Creates a brand new actor
-spec create(nkactor:actor(), nkactor:create_opts()) ->
    {ok, nkserver:id(), nkactor:actor(),  Meta::map()} | {error, actor_not_found|term()}.

create(Actor, #{activate:=false}=Opts) ->
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    SpanLocalId = maps:get(use_span_local_id, Opts, undefined),
    nkserver_ot:log(SpanLocalId, starting_actor_create_without_activation),
    case nkactor_util:pre_create(Actor, Syntax, Opts) of
        {ok, SrvId, Actor2} ->
            % Non recommended for non-relational databases, if name is not
            % randomly generated
            Config = maps:with([parent_span], Opts),
            case ?CALL_SRV(SrvId, actor_db_create, [SrvId, Actor2, Config]) of
                {ok, Meta} ->
                    % Use the alternative method for sending the event
                    nkactor_lib:send_external_event(SrvId, created, Actor2),
                    case Opts of
                        #{get_actor:=true} ->
                            ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                            {ok, SrvId, ActorId, Meta};
                        _ ->
                            {ok, SrvId, Actor2, Meta}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end;

create(Actor, Opts) ->
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    SpanLocalId = maps:get(use_span_local_id, Opts, undefined),
    nkserver_ot:log(SpanLocalId, <<"starting actor create">>),
    case nkactor_util:pre_create(Actor, Syntax, Opts) of
        {ok, SrvId, Actor2} ->
            % If we use the activate option, the object is first
            % registered with leader, so you cannot have two with same
            % name even on non-relational databases
            % The process will send the 'create' event in-server
            Config = maps:with([ttl, parent_span], Opts),
            case ?CALL_SRV(SrvId, actor_create, [Actor2, Config]) of
                {ok, #actor_id{pid=Pid}=ActorId} when is_pid(Pid) ->
                    case Opts of
                        #{get_actor:=true} ->
                            case nkactor_srv:sync_op(ActorId, get_actor, infinity) of
                                {ok, Actor3} ->
                                    {ok, SrvId, Actor3, #{}};
                                {error, Error} ->
                                    {error, Error}
                            end;
                        _ ->
                            {ok, SrvId, ActorId, #{}}
                    end;
                {error, actor_already_registered} ->
                    {error, uniqueness_violation};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @doc Updates an actor
%% It will activate the object, unless indicated
update(_Id, _Actor, #{activate:=false}) ->
    % TODO: perform the manual update?
    % nkactor_lib:send_external_event(SrvId, update, Actor2),
    {error, update_not_implemented};

update(Id, Actor, Opts) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_util:pre_update(ActorId, Syntax, Actor, Opts) of
        {ok, _SrvId, Actor2} ->
            case activate(ActorId, Opts) of
                {ok, SrvId, ActorId2, _} ->
                    UpdOpts = maps:get(update_opts, Opts, #{}),
                    case nkactor_srv:sync_op(ActorId2, {update, Actor2, UpdOpts}, infinity) of
                        ok ->
                            {ok, Actor3} = nkactor_srv:sync_op(ActorId2, get_actor, infinity),
                            {ok, SrvId, Actor3, #{}};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Deletes an actor
-spec delete(nkactor:id(), #{cascade=>boolean()}) ->
    {ok, [#actor_id{}], map()} | {error, actor_not_found|term()}.

delete(Id, Opts) ->
    case find(Id) of
        {ok, SrvId, #actor_id{uid=UID, pid=Pid}=ActorId2, _Meta} ->
            case maps:get(cascade, Opts, false) of
                false when is_pid(Pid) ->
                    case nkactor_srv:sync_op(ActorId2, delete, infinity) of
                        ok ->
                            % The object is loaded, and it will perform the delete
                            % itself, including sending the event (a full event)
                            % It will stop, and when the backend calls raw_stop/2
                            % the actor would not be activated, unless it is
                            % reactivated in the middle, and would stop without saving
                            {ok, [ActorId2], #{}};
                        {error, Error} ->
                            {error, Error}
                    end;
                Cascade ->
                    % The actor is not activated or we want cascade deletion
                    Opts2 = #{cascade => Cascade},
                    % Implementation must call nkactor_srv:raw_stop/1
                    case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, [UID], Opts2]) of
                        {ok, ActorIds, DeleteMeta} ->
                            % In this case, we must send the deleted events
                            lists:foreach(
                                fun(AId) ->
                                    FakeActor = make_fake_actor(AId),
                                    nkactor_lib:send_external_event(SrvId, deleted, FakeActor)
                                end,
                                ActorIds),
                            {ok, ActorIds, DeleteMeta};
                        {error, Error} ->
                            {error, Error}
                    end
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
%% Deletes a number of UIDs and send events
%% Loaded objects wil be unloaded
delete_multi(SrvId, UIDs) ->
    % Implementation must call nkactor_srv:raw_stop/1
    case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, UIDs, #{}]) of
        {ok, ActorIds, DeleteMeta} ->
            lists:foreach(
                fun(AId) ->
                    FakeActor = make_fake_actor(AId),
                    nkactor_lib:send_external_event(SrvId, deleted, FakeActor)
                end,
                ActorIds),
            {ok, ActorIds, DeleteMeta};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec search(nkactor:id(), search_type(), map()) ->
    {ok, [search_obj()], Meta::map()} | {error, term()}.

search(SrvId, SearchType, Opts) ->
    ?CALL_SRV(SrvId, actor_db_search, [SrvId, SearchType, Opts]).


%% @doc
-spec aggregation(nkactor:id(), agg_type(), map()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

aggregation(SrvId, AggType, Opts) ->
    ?CALL_SRV(SrvId, actor_db_aggregate, [SrvId, AggType, Opts]).



%%%% @doc Check if the info on database for service is up to date, and update it
%%%% - If the current info belongs to us (or it is missing), the time is updated
%%%% - If the info does not belong to us, but it is old (more than serviceDbMaxHeartbeatTime)
%%%%   it is overwritten
%%%% - If it is recent, and error alternate_service is returned
%%
%%-spec check_service(nkactor:id(), nkactor:id(), binary(), integer()) ->
%%    ok | {alternate_service, service_info()} | {error, term()}.
%%
%%check_service(SrvId, ActorSrvId, Cluster, MaxTime) ->
%%    Node = node(),
%%    Pid = self(),
%%    case ?CALL_SRV(SrvId, actor_db_get_service, [SrvId, ActorSrvId]) of
%%        {ok, #{cluster:=Cluster, node:=Node, pid:=Pid}, _} ->
%%            check_service_update(SrvId, ActorSrvId, Cluster);
%%        {ok, #{updated:=Updated}=Info, _} ->
%%            Now = nklib_date:epoch(secs),
%%            case Now - Updated < (MaxTime div 1000) of
%%                true ->
%%                    % It is recent, we consider it valid
%%                    {alternate_service, Info};
%%                false ->
%%                    % Too old, we overwrite it
%%                    check_service_update(SrvId, ActorSrvId, Cluster)
%%            end;
%%        {error, service_not_found} ->
%%            check_service_update(SrvId, ActorSrvId, Cluster);
%%        {error, Error} ->
%%            {error, Error}
%%    end.
%%
%%
%%%% @private
%%check_service_update(SrvId, ActorSrvId, Cluster) ->
%%    case ?CALL_SRV(SrvId, actor_db_update_service, [SrvId, ActorSrvId, Cluster]) of
%%        {ok, _} ->
%%            ok;
%%        {error, Error} ->
%%            {error, Error}
%%    end.





%% ===================================================================
%% Internal
%% ===================================================================

do_read(SrvId, ActorId, Opts) ->
    case ?CALL_SRV(SrvId, actor_db_read, [SrvId, ActorId, #{}]) of
        {ok, Actor, Meta} ->
            #actor_id{group = Group, resource = Res} = ActorId,
            case nkactor_util:get_module(SrvId, Group, Res) of
                undefined ->
                    ?LLOG(warning, "actor resource unknown ~s:~s", [Group, Res]),
                    {error, actor_invalid};
                Module ->
                    Syntax = #{
                        '__mandatory' => [group, resource, name, namespace, uid]
                    },
                    case nkactor_syntax:parse_actor(Actor, Syntax) of
                        {ok, Actor2} ->
                            Req1 = maps:get(request, Opts, #{}),
                            Req2 = Req1#{
                                verb => get,
                                srv => SrvId
                            },
                            case nkactor_actor:parse(Module, Actor2, Req2) of
                                {ok, Actor3} ->
                                    {ok, Actor3, Meta};
                                {error, Error} ->
                                    {error, Error}
                            end;
                        {error, Error} ->
                            {error, Error}
                    end
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
make_fake_actor(ActorId) ->
    #actor_id{
        group = Group,
        resource = Resource,
        name = Name,
        %vsn = Vsn,
        namespace = Namespace,
        uid = UID
    } = ActorId,
    #{
        group => Group,
        resource => Resource,
        name => Name,
        namespace => Namespace,
        %vsn => Vsn,
        uid => UID
    }.
