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

-export([find/2, activate/2, read/2]).
-export([create/2, update/3, delete/2, delete_multi/2]).
-export([search/3, aggregation/3, truncate/2]).
-export([search_activate_actors/3]).
%% -export([check_service/4]).
%%-export_type([search_obj/0, search_objs_opts/0]).

-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").

-define(ACTIVATE_SPAN, auto_activate).


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
%% If an 'OptSrvId' is used, it will be used to focus the search on that
%% service if namespace is not found
%%
%% If 'ot_span_id' is defined, logs will be added

-spec find(nkactor:id(), map()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

find({OptSrvId, Id}, Opts) when is_atom(OptSrvId) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    nkserver_ot:log(SpanId, <<"calling find actor">>),
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case nkactor_namespace:find_actor(ActorId) of
        {true, SrvId, #actor_id{pid=Pid}=ActorId2} when is_pid(Pid) ->
            % It is registered or cached
            nkserver_ot:log(SpanId, <<"actor is registered or cached">>),
            {ok, SrvId, ActorId2, #{}};
        {false, SrvId} ->
            do_find([SrvId], ActorId, SpanId);
        false when OptSrvId==undefined ->
            SrvIds = nkactor:get_services(),
            do_find(SrvIds, ActorId, SpanId);
        false ->
            do_find([OptSrvId], ActorId, SpanId)
    end;

find(Id, Opts) ->
    find({undefined, Id}, Opts).


%% @private
do_find([], _ActorId, _SpanId) ->
    {error, actor_not_found};

do_find([SrvId|Rest], ActorId, SpanId) ->
    nkserver_ot:log(SpanId, <<"calling actor_db_find for ~s">>, [SrvId]),
    case ?CALL_SRV(SrvId, actor_db_find, [SrvId, ActorId, #{ot_span_id=>SpanId}]) of
        {ok, #actor_id{} = ActorId2, Meta} ->
            % If its was an UID, or a partial path, we must check now that
            % it could be registered, now that we have full data
            case nkactor_namespace:find_actor(ActorId2) of
                {true, _SrvId, #actor_id{pid = Pid} = ActorId3} when is_pid(Pid) ->
                    nkserver_ot:log(SpanId, <<"actor found in disk and memory: ~p">>, [ActorId3]),
                    {ok, SrvId, ActorId3, Meta};
                _ ->
                    nkserver_ot:log(SpanId, <<"actor found in disk: ~p">>, [ActorId2]),
                    {ok, SrvId, ActorId2, Meta}
            end;
        {error, actor_not_found} ->
            nkserver_ot:log(SpanId, <<"actor not found in disk">>),
            do_find(Rest, ActorId, SpanId);
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error calling actor_db_find: ~p">>, [Error]),
            {error, Error}
    end.



%% @doc Finds an actors's pid or loads it from storage and activates it
%% See description for get_opts()
%% If 'ot_span_id' is defined, logs will be added
-spec activate(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

activate(Id, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case do_activate(Id, Opts#{ot_span_id=>SpanId}) of
        {ok, SrvId, ActorId2, Meta2} ->
            nkserver_ot:log(SpanId, <<"actor is activated">>),
            {ok, SrvId, ActorId2, Meta2};
        {error, actor_not_found} ->
            {error, actor_not_found};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Reads an actor from memory if loaded, or disk if not
%% It will first try to activate it (unless indicated)
%% If consume is set, it will destroy the object on read
%% SrvId is used for calling the DB
%%
%% See description for get_opts()

-spec read(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkactor:actor(),  Meta::map()} | {error, actor_not_found|term()}.

read(Id, #{activate:=false}=Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case maps:get(consume, Opts, false) of
        true ->
            {error, cannot_consume};
        false ->
            Opts2 = Opts#{ot_span_id=>SpanId},
            case find(Id, Opts2) of
                {ok, SrvId, ActorId, _} ->
                    case do_read(SrvId, ActorId, Opts2) of
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
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case do_activate(Id, Opts#{ot_span_id=>SpanId}) of
        {ok, SrvId, ActorId2, Meta2} ->
            Op = case maps:get(consume, Opts, false) of
                true ->
                    consume_actor;
                false ->
                    get_actor
            end,
            case nkactor:sync_op(ActorId2, Op, infinity) of
                {ok, Actor} ->
                    {ok, SrvId, Actor, Meta2};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Creates a brand new actor
%% It will generate a new span
-spec create(nkactor:actor(), nkactor:create_opts()) ->
    {ok, nkserver:id(), nkactor:actor(),  Meta::map()} | {error, actor_not_found|term()}.

create(Actor, #{activate:=false}=Opts) ->
    span_create(create, undefined, Opts),
    case pre_create(Actor, Opts) of
        {ok, SrvId, Actor2} ->
            #{
                namespace := Namespace,
                group := Group,
                resource := Res,
                name := Name,
                uid := UID
            } = Actor2,
            span_update_srv_id(create, SrvId),
            span_tags(create, #{
                <<"actor.namespace">> => Namespace,
                <<"actor.group">> => Group,
                <<"actor.resource">> => Res,
                <<"actor.name">> => Name,
                <<"actor.uid">> => UID,
                <<"actor.opts.activate">> => false
            }),
            % Non recommended for non-relational databases, if name is not
            % randomly generated
            Config1 = maps:with([no_unique_check], Opts),
            Config2 = Config1#{ot_span_id=>span_id(create)},
            span_log(create, <<"calling actor_db_create">>),
            case ?CALL_SRV(SrvId, actor_db_create, [SrvId, Actor2, Config2]) of
                {ok, Meta} ->
                    % Use the alternative method for sending the event
                    nkactor_lib:send_external_event(SrvId, created, Actor2),
                    case Opts of
                        #{get_actor:=true} ->
                            span_finish(create),
                            {ok, SrvId, Actor2, Meta};
                        _ ->
                            ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                            span_finish(create),
                            {ok, SrvId, ActorId, Meta}
                    end;
                {error, Error} ->
                    span_log(create, <<"error creating actor: ~p">>, [Error]),
                    span_error(create, Error),
                    span_finish(create),
                    {error, Error}
            end;
        {error, Error} ->
            span_delete(create),
            {error, Error}
    end;

create(Actor, Opts) ->
    span_create(create, undefined, Opts),
    case pre_create(Actor, Opts#{ot_span_id=>span_id(create)}) of
        {ok, SrvId, Actor2} ->
            span_update_srv_id(create, SrvId),
            % If we use the activate option, the object is first
            % registered with leader, so you cannot have two with same
            % name even on non-relational databases
            % The process will send the 'create' event in-server
            Config1 = maps:with([ttl, no_unique_check], Opts),
            Config2 = Config1#{ot_span_id=>span_id(create)},
            span_log(create, <<"calling actor create">>),
            case ?CALL_SRV(SrvId, actor_create, [Actor2, Config2]) of
                {ok, Pid} when is_pid(Pid) ->
                    ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                    #actor_id{
                        namespace = Namespace,
                        group = Group,
                        resource = Res,
                        name = Name,
                        uid = UID
                    } = ActorId,
                    span_tags(create, #{
                        <<"actor.namespace">> => Namespace,
                        <<"actor.group">> => Group,
                        <<"actor.resource">> => Res,
                        <<"actor.name">> => Name,
                        <<"actor.uid">> => UID,
                        <<"actor.pid">> => list_to_binary(pid_to_list(Pid)),
                        <<"actor.opts.activate">> => true
                    }),
                    case Opts of
                        #{get_actor:=true} ->
                            span_log(create, <<"calling get_actor">>),
                            case nkactor:sync_op(ActorId, get_actor, infinity) of
                                {ok, Actor3} ->
                                    span_finish(create),
                                    {ok, SrvId, Actor3, #{}};
                                {error, Error} ->
                                    span_log(create, <<"error getting actor: ~p">>, [Error]),
                                    span_error(create, Error),
                                    span_finish(create),
                                    {error, Error}
                            end;
                        _ ->
                            span_finish(create),
                            {ok, SrvId, ActorId, #{}}
                    end;
                {error, Error} ->
                    span_log(create, <<"error creating actor: ~p">>, [Error]),
                    span_error(create, Error),
                    span_finish(create),
                    {error, Error}
            end;
        {error, Error} ->
            span_delete(create),
            {error, Error}
    end.



%% @doc Updates an actor
%% It will activate the object, unless indicated
update(_Id, _Actor, #{activate:=false}) ->
    % TODO: perform the manual update?
    % nkactor_lib:send_external_event(SrvId, update, Actor2),
    {error, update_not_implemented};

update(Id, Actor, Opts) ->
    span_create(update, undefined, Opts),
    case do_activate(Id, Opts#{ot_span_id=>span_id(update)}) of
        {ok, SrvId, ActorId, _} ->
            span_update_srv_id(update, SrvId),
            #actor_id{
                namespace = Namespace,
                group = Group,
                resource = Res,
                name = Name,
                uid = UID,
                pid = Pid
            } = ActorId,
            span_tags(update, #{
                <<"actor.namespace">> => Namespace,
                <<"actor.group">> => Group,
                <<"actor.resource">> => Res,
                <<"actor.name">> => Name,
                <<"actor.uid">> => UID,
                <<"actor.pid">> => list_to_binary(pid_to_list(Pid)),
                <<"actor.opts.activate">> => true
            }),
            Opts2 = Opts#{ot_span_id=>span_id(update)},
            span_log(update, <<"calling update actor">>),
            case pre_update(SrvId, ActorId, Actor, Opts#{ot_span_id=>span_id(update)}) of
                {ok, Actor2} ->
                    case nkactor:sync_op(ActorId, {update, Actor2, Opts2}, infinity) of
                        {ok, Actor3} ->
                            span_finish(update),
                            {ok, SrvId, Actor3, #{}};
                        {error, Error} ->
                            span_log(update, <<"error calling actor update actor: ~p">>, [Error]),
                            span_error(update, Error),
                            span_finish(update),
                            {error, Error}
                    end;
                {error, Error} ->
                    span_log(update, <<"error updating actor: ~p">>, [Error]),
                    span_error(update, Error),
                    span_finish(update),
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Deletes an actor
-spec delete(nkactor:id(), #{cascade=>boolean()}) ->
    {ok, map()} | {error, actor_not_found|term()}.

delete(Id, Opts) ->
    span_create(delete, undefined, Opts),
    case find(Id, Opts#{ot_span_id=>span_id(delete)}) of
        {ok, SrvId, #actor_id{uid=UID, pid=Pid}=ActorId2, _Meta} ->
            span_update_srv_id(delete, SrvId),
            #actor_id{
                namespace = Namespace,
                group = Group,
                resource = Res,
                name = Name,
                uid = UID,
                pid = Pid
            } = ActorId2,
            PidBin = case is_pid(Pid) of
                true ->
                    list_to_binary(pid_to_list(Pid));
                false ->
                    <<>>
            end,
            span_tags(delete, #{
                <<"actor.namespace">> => Namespace,
                <<"actor.group">> => Group,
                <<"actor.resource">> => Res,
                <<"actor.name">> => Name,
                <<"actor.uid">> => UID,
                <<"actor.pid">> => PidBin
            }),
            case is_pid(Pid) of
                true ->
                    span_log(delete, <<"calling actor delete">>),
                    case nkactor:sync_op(ActorId2, delete, infinity) of
                        ok ->
                            % The object is loaded, and it will perform the delete
                            % itself, including sending the event (a full event)
                            % It will stop, and when the backend calls raw_stop/2
                            % the actor would not be activated, unless it is
                            % reactivated in the middle, and would stop without saving
                            span_log(delete, <<"successful">>),
                            span_finish(delete),
                            {ok, #{}};
                        {error, Error} ->
                            span_log(delete, <<"error deleting active actor: ~p">>, [Error]),
                            span_error(delete, Error),
                            span_finish(delete),
                            {error, Error}
                    end;
                false ->
                    span_log(delete, <<"calling actor_db_delete">>),
                    case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, ActorId2, Opts]) of
                        {ok, DeleteMeta} ->
                            % In this case, we must send the deleted events
                            FakeActor = make_fake_actor(ActorId2),
                            nkactor_lib:send_external_event(SrvId, deleted, FakeActor),
                            span_log(delete, <<"successful">>),
                            span_finish(delete),
                            {ok, DeleteMeta};
                        {error, Error} ->
                            span_log(delete, <<"error deleting inactive actor: ~p">>, [Error]),
                            span_error(delete, Error),
                            span_finish(delete),
                            {error, Error}
                    end
            end;
        {error, Error} ->
            % We didn't identify any service
            span_delete(delete),
            {error, Error}
    end.


%% @doc
delete_multi(SrvId, ActorIds) ->
    ?CALL_SRV(SrvId, actor_db_delete_multi, [SrvId, ActorIds, #{}]).




%%
%%%% @doc Deletes an actor
%%-spec delete(nkactor:id(), #{cascade=>boolean()}) ->
%%    {ok, #actor_id{}, map()} | {error, actor_not_found|term()}.
%%
%%delete(Id, Opts) ->
%%    span_create(delete, undefined, Opts),
%%    case find(Id, Opts#{ot_span_id=>?BACKEND_SPAN}) of
%%        {ok, SrvId, #actor_id{uid=UID, pid=Pid}=ActorId2, _Meta} ->
%%            span_update_srv_id(SrvId),
%%            #actor_id{
%%                namespace = Namespace,
%%                group = Group,
%%                resource = Res,
%%                name = Name,
%%                uid = UID,
%%                pid = Pid
%%            } = ActorId2,
%%            PidBin = case is_pid(Pid) of
%%                true ->
%%                    list_to_binary(pid_to_list(Pid));
%%                false ->
%%                    <<>>
%%            end,
%%            nkserver_ot:tags(?BACKEND_SPAN, #{
%%                <<"actor.namespace">> => Namespace,
%%                <<"actor.group">> => Group,
%%                <<"actor.resource">> => Res,
%%                <<"actor.name">> => Name,
%%                <<"actor.uid">> => UID,
%%                <<"actor.pid">> => PidBin,
%%                <<"actor.opts.activate">> => true
%%            }),
%%            case maps:get(cascade, Opts, false) of
%%                false when is_pid(Pid) ->
%%                    span_log(<<"cascade: false (active)">>),
%%                    span_log(<<"calling actor_delete">>),
%%                    lager:error("NKLOG DELETE3"),
%%                    case nkactor:sync_op(ActorId2, delete, infinity) of
%%                        ok ->
%%                            % The object is loaded, and it will perform the delete
%%                            % itself, including sending the event (a full event)
%%                            % It will stop, and when the backend calls raw_stop/2
%%                            % the actor would not be activated, unless it is
%%                            % reactivated in the middle, and would stop without saving
%%                            span_log(<<"successful">>),
%%                            span_finish(),
%%                            {ok, [ActorId2], #{}};
%%                        {error, Error} ->
%%                            span_log(<<"error deleting active actor: ~p">>, [Error]),
%%                            span_error(Error),
%%                            span_finish(),
%%                            {error, Error}
%%                    end;
%%                Cascade ->
%%                    span_log(<<"cascade: ~p">>, [Cascade]),
%%                    % The actor is not activated or we want cascade deletion
%%                    Opts2 = #{cascade => Cascade},
%%                    % Implementation must call nkactor_srv:raw_stop/1
%%                    span_log(<<"calling actor_db_delete">>),
%%                    case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, UID, Opts2]) of
%%                        {ok, DeleteMeta} ->
%%                            % In this case, we must send the deleted events
%%                            FakeActor = make_fake_actor(ActorId),
%%                            nkactor_lib:send_external_event(SrvId, deleted, FakeActor),
%%                            span_log(<<"successful">>),
%%                            span_finish(),
%%                            {ok, ActorId, DeleteMeta};
%%                        {error, Error} ->
%%                            span_log(<<"error deleting inactive actor: ~p">>, [Error]),
%%                            span_error(Error),
%%                            span_finish(),
%%                            {error, Error}
%%                    end
%%            end;
%%        {error, Error} ->
%%            span_delete(),
%%            {error, Error}
%%    end.
%%
%%
%%%% @doc
%%%% Deletes a number of UIDs and send events
%%%% Loaded objects wil be unloaded
%%delete_multi(SrvId, UIDs, Opts) ->
%%    % Implementation must call nkactor_srv:raw_stop/1
%%    span_create(delete_multi, undefined, Opts),
%%    span_log(<<"uids: ~p">>, [UIDs]),
%%    span_log(<<"calling actor_db_delete">>),
%%    case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, UIDs, #{}]) of
%%        {ok, ActorIds, DeleteMeta} ->
%%            lists:foreach(
%%                fun(AId) ->
%%                    FakeActor = make_fake_actor(AId),
%%                    nkactor_lib:send_external_event(SrvId, deleted, FakeActor)
%%                end,
%%                ActorIds),
%%            span_log(<<"success">>),
%%            span_finish(),
%%            {ok, ActorIds, DeleteMeta};
%%        {error, Error} ->
%%            span_log(<<"error deleting multi actor: ~p">>, [Error]),
%%            span_error(Error),
%%            span_finish(),
%%            {error, Error}
%%    end.


%% @doc
-spec search(nkactor:id(), search_type(), map()) ->
    {ok, [search_obj()], Meta::map()} | {error, term()}.

search(SrvId, SearchType, Opts) ->
    span_create(search, SrvId, Opts),
    span_log(search, "search type: ~p (~p)", [SearchType, Opts]),
    span_log(search, "calling actor_db_search"),
    Opts2 = Opts#{ot_span_id=>span_id(search)},
    case ?CALL_SRV(SrvId, actor_db_search, [SrvId, SearchType, Opts2]) of
        {ok, Actors, Meta} ->
            span_log(search, "success: ~p", [Meta]),
            span_finish(search),
            case parse_actors(SrvId, Actors, Opts) of
                {ok, Actors2} ->
                    {ok, Actors2, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            span_log(search, <<"error in search: ~p">>, [Error]),
            span_error(search, Error),
            span_finish(search),
            {error, Error}
    end.


%% @doc
-spec aggregation(nkactor:id(), agg_type(), map()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

aggregation(SrvId, AggType, Opts) ->
    span_create(agg, SrvId, Opts),
    span_log(agg, "agg type: ~p (~p)", [AggType, Opts]),
    span_log(agg, "calling actor_db_aggregate"),
    Opts2 = Opts#{ot_span_id=>span_id(agg)},
    case ?CALL_SRV(SrvId, actor_db_aggregate, [SrvId, AggType, Opts2]) of
        {ok, Result, Meta} ->
            span_log(agg, "success: ~p", [Meta]),
            span_finish(agg),
            {ok, Result, Meta};
        {error, Error} ->
            span_log(agg, <<"error in agg: ~p">>, [Error]),
            span_error(agg, Error),
            span_finish(agg),
            {error, Error}
    end.


%% @doc
-spec truncate(nkactor:id(), map()) ->
    ok | {error, term()}.

truncate(SrvId, Opts) ->
    span_create(truncate, SrvId, Opts),
    span_log(truncate, "truncate (~p)", [Opts]),
    span_log(truncate, "calling actor_db_truncate"),
    Opts2 = Opts#{ot_span_id=>span_id(truncate)},
    case ?CALL_SRV(SrvId, actor_db_truncate, [SrvId, Opts2]) of
        ok ->
            span_log(truncate, "success"),
            span_finish(truncate),
            ok;
        {error, Error} ->
            span_log(truncate, <<"error in trunctae: ~p">>, [Error]),
            span_error(truncate, Error),
            span_finish(truncate),
            {error, Error}
    end.


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
do_read(SrvId, ActorId, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    nkserver_ot:log(SpanId, <<"calling actor_db_read">>),
    case ?CALL_SRV(SrvId, actor_db_read, [SrvId, ActorId, Opts]) of
        {ok, Actor, Meta} ->
            % Actor's generic syntax is already parsed
            % Now we check specific syntax
            % If request option is provided, it is used for parsing
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            Req3 = maps:merge(#{ot_span_id=>SpanId}, Req2),
            nkserver_ot:log(SpanId, <<"calling actor parse">>),
            case Opts of
                #{no_data_parse:=true} ->
                    {ok, Actor, Meta};
                _ ->
                    case nkactor_actor:parse(SrvId, read, Actor, Req3) of
                        {ok, Actor2} ->
                            nkserver_ot:log(SpanId, <<"actor is valid">>),
                            {ok, Actor2, Meta};
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error parsing actor: ~p">>, [Error]),
                            {error, Error}
                    end
            end;
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error readinf actor: ~p">>, [Error]),
            {error, Error}
    end.


%% @private
do_activate(Id, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case find(Id, Opts) of
        {ok, SrvId, #actor_id{pid=Pid}=ActorId, Meta} when is_pid(Pid) ->
            {ok, SrvId, ActorId, Meta};
        {ok, SrvId, ActorId, _Meta} ->
            case do_read(SrvId, ActorId, Opts) of
                {ok, Actor, Meta2} ->
                    nkserver_ot:log(SpanId, <<"calling actor_activate">>),
                    Config1 = maps:with([ttl], Opts),
                    Config2 = Config1#{ot_span_id=>SpanId},
                    case ?CALL_SRV(SrvId, actor_activate, [Actor, Config2]) of
                        {ok, Pid} ->
                            nkserver_ot:log(SpanId, <<"actor is activated">>),
                            {ok, SrvId, ActorId#actor_id{pid=Pid}, Meta2};
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error activating actor: ~p">>, [Error]),
                            {error, Error}
                    end;
                {error, persistence_not_defined} ->
                    nkserver_ot:log(SpanId, <<"error activating actor: actor_not_found">>),
                    {error, actor_not_found};
                {error, Error} ->
                    nkserver_ot:log(SpanId, <<"error activating actor: ~p">>, [Error]),
                    {error, Error}
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
        namespace = Namespace,
        uid = UID
    } = ActorId,
    #{
        group => Group,
        resource => Resource,
        name => Name,
        namespace => Namespace,
        uid => UID
    }.


%% @doc Performs an query on database for actors due for activation
-spec search_activate_actors(nkserver:id(), Date::binary(), PageSize::pos_integer()) ->
    {ok, [nkactor:uid()]} | {error, term()}.

search_activate_actors(SrvId, Date, PageSize) ->
    nkserver_ot:new(?ACTIVATE_SPAN, SrvId, <<"Actor::auto-activate">>),
    Res = search_activate_actors(SrvId, #{last_time=>Date, size=>PageSize}, 100, []),
    nkserver_ot:finish(?ACTIVATE_SPAN),
    Res.


%% @private
search_activate_actors(SrvId, Opts, Iters, Acc) when Iters > 0 ->
    nkserver_ot:log(?ACTIVATE_SPAN, {"starting cursor: ~s", [maps:get(last_time, Opts, <<>>)]}),
    ParentSpan = nkserver_ot:make_parent(?ACTIVATE_SPAN),
    Opts2 =  Opts#{ot_span_id=>ParentSpan},
    case search(SrvId, actors_activate, Opts2) of
        {ok, [], _} ->
            nkserver_ot:log(?ACTIVATE_SPAN, <<"no more actors">>),
            {ok, lists:flatten(Acc)};
        {ok, ActorIds, #{last_time:=LastDate}} ->
            nkserver_ot:log(?ACTIVATE_SPAN, {"found '~p' actors", [length(ActorIds)]}),
            search_activate_actors(SrvId, Opts#{last_time=>LastDate}, Iters-1, [ActorIds|Acc]);
        {error, Error} ->
            {error, Error}
    end;

search_activate_actors(_SrvId, _Opts, _Iters, _Acc) ->
    {error, too_many_iterations}.


%% @private
parse_actors(SrvId, Actors, Opts) ->
    Req = maps:get(request, Opts, #{}),
    parse_actors(SrvId, Actors, Req, []).


%% @private
parse_actors(_SrvId, [], _Req, Acc) ->
    {ok, lists:reverse(Acc)};

parse_actors(SrvId, [#{data:=Data, metadata:=Meta}=Actor|Rest], Req, Acc)
        when map_size(Data) > 0, map_size(Meta) > 0 ->
    case nkactor_actor:parse(SrvId, read, Actor, Req) of
        {ok, Actor2} ->
            parse_actors(SrvId, Rest, Req, [Actor2|Acc]);
        {error, Error} ->
            lager:error("NkACTOR error ~p parsing ~p", [Error, Actor]),
            {error, Error}
    end;

parse_actors(SrvId, [Actor|Rest], Req, Acc) ->
    parse_actors(SrvId, Rest, Req, [Actor|Acc]).

%% @private
pre_create(Actor, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            nkserver_ot:log(SpanId, <<"actor parsed">>),
            #{namespace:=Namespace} = Actor2,
            case nkactor_namespace:find_service(Namespace) of
                {ok, SrvId} ->
                    nkserver_ot:log(SpanId, <<"actor namespace found: ~s">>, [SrvId]),
                    case nkactor_lib:add_creation_fields(SrvId, Actor2) of
                        {ok, Actor3} ->
                            Actor4 = case Opts of
                                #{forced_uid:=UID} ->
                                    nomatch = binary:match(UID, <<".">>),
                                    <<First, _/binary>> = UID,
                                    true = (First /= $/),
                                    Actor3#{uid := UID};
                                _ ->
                                    Actor3
                            end,
                            Req1 = maps:get(request, Opts, #{}),
                            Req2 = Req1#{srv => SrvId},
                            case nkactor_actor:parse(SrvId, create, Actor4, Req2) of
                                {ok, Actor5} ->
                                    case nkactor_lib:check_actor_links(Actor5) of
                                        {ok, Actor6} ->
                                            nkserver_ot:log(SpanId, <<"actor parsed">>),
                                            {ok, SrvId, Actor6};
                                        {error, Error} ->
                                            nkserver_ot:log(SpanId, <<"error checking links: ~p">>, [Error]),
                                            {error, Error}
                                    end;
                                {error, Error} ->
                                    nkserver_ot:log(SpanId, <<"error parsing specific actor: ~p">>, [Error]),
                                    {error, Error}
                            end;
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error creating initial data: ~p">>, [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    nkserver_ot:log(SpanId, <<"error getting namespace: ~p">>, [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing generic actor: ~p">>, [Error]),
            {error, Error}
    end.


%% @private
pre_update(SrvId, ActorId, Actor, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case nkactor_syntax:parse_actor(Actor, #{}) of
        {ok, Actor2} ->
            #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
            Base = #{group=>Group, resource=>Res, name=>Name, namespace=>Namespace},
            Actor3 = maps:merge(Base, Actor2),
            nkserver_ot:log(SpanId, <<"actor parsed">>),
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case nkactor_actor:parse(SrvId, update, Actor3, Req2) of
                {ok, Actor4} ->
                    case nkactor_lib:check_actor_links(Actor4) of
                        {ok, Actor5} ->
                            nkserver_ot:log(SpanId, <<"actor parsed">>),
                            {ok, Actor5};
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error checking links: ~p">>, [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    nkserver_ot:log(SpanId, <<"error parsing specific actor: ~p">>, [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing generic actor: ~p">>, [Error]),
            {error, Error}
    end.


%% @private
span_create(Op, SrvId, Opts) ->
    Parent = maps:get(ot_span_id, Opts, undefined),
    Name = <<"ActorBackend::", (nklib_util:to_binary(Op))/binary>>,
    nkserver_ot:new(span_id(Op), SrvId, Name, Parent).


%% @private
span_id(Op) ->
    {?MODULE, Op}.


%% @private
span_finish(Op) ->
    nkserver_ot:finish(span_id(Op)).


%% @private
span_log(Op, Log) ->
    nkserver_ot:log(span_id(Op), Log).


%% @private
span_log(Op, Txt, Data) ->
    nkserver_ot:log(span_id(Op), Txt, Data).


%% @private
span_tags(Op, Tags) ->
    nkserver_ot:tags(span_id(Op), Tags).


%% @private
span_error(Op, Error) ->
    nkserver_ot:tag_error(span_id(Op), Error).


%% @private
span_update_srv_id(Op, SrvId) ->
    nkserver_ot:update_srv_id(span_id(Op), SrvId).


%% @private
span_delete(Op) ->
    nkserver_ot:delete(span_id(Op)).
