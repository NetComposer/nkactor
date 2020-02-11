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
-import(nkserver_trace, [event/2, log/2, log/3]).

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

find({OptSrvId, Id}, _Opts) when is_atom(OptSrvId) ->
    log(info, "calling find actor"),
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case nkactor_namespace:find_actor(ActorId) of
        {true, SrvId, #actor_id{pid=Pid}=ActorId2} when is_pid(Pid) ->
            % It is registered or cached
            log(info, "actor is registered or cached"),
            {ok, SrvId, ActorId2, #{}};
        {false, SrvId} ->
            do_find([SrvId], ActorId);
        false when OptSrvId==undefined ->
            SrvIds = nkactor:get_services(),
            do_find(SrvIds, ActorId);
        false ->
            do_find([OptSrvId], ActorId)
    end;

find(Id, Opts) ->
    find({undefined, Id}, Opts).


%% @private
do_find([], _ActorId) ->
    {error, actor_not_found};

do_find([SrvId|Rest], ActorId) ->
    log(info, "calling actor_db_find for ~p (~s)", [ActorId, SrvId]),
    case ?CALL_SRV(SrvId, actor_db_find, [SrvId, ActorId, #{}]) of
        {ok, #actor_id{} = ActorId2, Meta} ->
            % If its was an UID, or a partial path, we must check now that
            % it could be registered, now that we have full data
            case nkactor_namespace:find_actor(ActorId2) of
                {true, _SrvId, #actor_id{pid = Pid} = ActorId3} when is_pid(Pid) ->
                    log(info, "actor found in disk and memory: ~p", [ActorId3]),
                    {ok, SrvId, ActorId3, Meta};
                _ ->
                    log(info, "actor found in disk: ~p", [ActorId2] ),
                    {ok, SrvId, ActorId2, Meta}
            end;
        {error, actor_not_found} ->
            log(info, "actor not found in disk"),
            do_find(Rest, ActorId);
        {error, Error} ->
            log(info, "error calling actor_db_find: ~p", [Error]),
            {error, Error}
    end.



%% @doc Finds an actors's pid or loads it from storage and activates it
%% See description for get_opts()
%% If 'ot_span_id' is defined, logs will be added
-spec activate(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

activate(Id, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case do_activate(Id, Opts#{ot_span_id=>SpanId}, 3) of
        {ok, SrvId, ActorId2, Meta2} ->
            log(info, "actor is activated"),
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
    case do_activate(Id, Opts#{ot_span_id=>SpanId}, 3) of
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
    Fun = fun() ->
        case pre_create(Actor, Opts) of
            {ok, SrvId, Actor2} ->
                #{
                    namespace := Namespace,
                    group := Group,
                    resource := Res,
                    name := Name,
                    uid := UID
                } = Actor2,
                nkserver_trace:tags(#{
                    <<"actor.namespace">> => Namespace,
                    <<"actor.group">> => Group,
                    <<"actor.resource">> => Res,
                    <<"actor.name">> => Name,
                    <<"actor.uid">> => UID,
                    <<"actor.opts.activate">> => false
                }),
                % Non recommended for non-relational databases, if name is not
                % randomly generated
                Config = maps:with([no_unique_check], Opts),
                log(info, "calling actor_db_create"),
                case ?CALL_SRV(SrvId, actor_db_create, [SrvId, Actor2, Config]) of
                    {ok, Meta} ->
                        % Use the alternative method for sending the event
                        nkactor_lib:send_external_event(SrvId, created, Actor2),
                        case Opts of
                            #{get_actor:=true} ->
                                {ok, SrvId, Actor2, Meta};
                            _ ->
                                ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                                {ok, SrvId, ActorId, Meta}
                        end;
                    {error, Error} ->
                        log("error creating actor: ~p", [Error]),
                        nkserver_trace:error(Error),
                        {error, Error}
                end;
            {error, Error} ->
                %span_delete(create),
                {error, Error}
        end
    end,
    nkserver_trace:new(same, "ActorBackend::create", Fun, #{});

create(Actor, Opts) ->
    Fun = fun() ->
        case pre_create(Actor, Opts) of
            {ok, SrvId, Actor2} ->
                % If we use the activate option, the object is first
                % registered with leader, so you cannot have two with same
                % name even on non-relational databases
                % The process will send the 'create' event in-server
                Config = maps:with([ttl, no_unique_check], Opts),
                log(info, "calling actor create"),
                case ?CALL_SRV(SrvId, actor_create, [Actor2, Config]) of
                    {ok, Pid} when is_pid(Pid) ->
                        ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                        #actor_id{
                            namespace = Namespace,
                            group = Group,
                            resource = Res,
                            name = Name,
                            uid = UID
                        } = ActorId,
                        nkserver_trace:tags(#{
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
                                log(info, "calling get_actor"),
                                case nkactor:sync_op(ActorId, get_actor, infinity) of
                                    {ok, Actor3} ->
                                        {ok, SrvId, Actor3, #{}};
                                    {error, Error} ->
                                        log(info, "error getting actor: ~p", [Error]),
                                        nkserver_trace:error(Error),
                                        {error, Error}
                                end;
                            _ ->
                                {ok, SrvId, ActorId, #{}}
                        end;
                    {error, Error} ->
                        log(info, "error creating actor: ~p", [Error]),
                        nkserver_trace:error(Error),
                        case Error of
                            actor_already_activated ->
                                {error, actor_already_exists};
                            _ ->
                                {error, Error}
                        end
                end;
            {error, Error} ->
                %span_delete(create),
                {error, Error}
        end
    end,
    nkserver_trace:new(same, "ActorBackend::create", Fun, #{}).


%% @doc Updates an actor
%% It will activate the object, unless indicated
update(_Id, _Actor, #{activate:=false}) ->
    % TODO: perform the manual update?
    % nkactor_lib:send_external_event(SrvId, update, Actor2),
    {error, update_not_implemented};

update(Id, Actor, Opts) ->
    Fun = fun() ->
        case do_activate(Id, Opts, 3) of
            {ok, SrvId, ActorId, _} ->
                #actor_id{
                    namespace = Namespace,
                    group = Group,
                    resource = Res,
                    name = Name,
                    uid = UID,
                    pid = Pid
                } = ActorId,
                nkserver_trace:tags(#{
                    <<"actor.namespace">> => Namespace,
                    <<"actor.group">> => Group,
                    <<"actor.resource">> => Res,
                    <<"actor.name">> => Name,
                    <<"actor.uid">> => UID,
                    <<"actor.pid">> => list_to_binary(pid_to_list(Pid)),
                    <<"actor.opts.activate">> => true
                }),
                log(info, "calling update actor"),
                case pre_update(SrvId, ActorId, Actor, Opts) of
                    {ok, Actor2} ->
                        case nkactor:sync_op(ActorId, {update, Actor2, Opts}, infinity) of
                            {ok, Actor3} ->
                                {ok, SrvId, Actor3, #{}};
                            {error, Error} ->
                                log(update, "error calling actor update actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end;
                    {error, Error} ->
                        log(info, "error updating actor: ~p", [Error]),
                        nkserver_trace:error(Error),
                        {error, Error}
                end;
            {error, Error} ->
                {error, Error}
        end
    end,
    nkserver_trace:new(same, "ActorBackend::update", Fun, #{}).



%% @doc Deletes an actor
-spec delete(nkactor:id(), #{cascade=>boolean()}) ->
    {ok, map()} | {error, actor_not_found|term()}.

delete(Id, Opts) ->
    Fun = fun() ->
        case find(Id, Opts) of
            {ok, SrvId, #actor_id{uid=UID, pid=Pid}=ActorId2, _Meta} ->
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
                nkserver_trace:tags(#{
                    <<"actor.namespace">> => Namespace,
                    <<"actor.group">> => Group,
                    <<"actor.resource">> => Res,
                    <<"actor.name">> => Name,
                    <<"actor.uid">> => UID,
                    <<"actor.pid">> => PidBin
                }),
                case is_pid(Pid) of
                    true ->
                        log(info, "calling actor delete"),
                        case nkactor:sync_op(ActorId2, {delete, Opts}, infinity) of
                            ok ->
                                % The object is loaded, and it will perform the delete
                                % itself, including sending the event (a full event)
                                % It will stop, and when the backend calls raw_stop/2
                                % the actor would not be activated, unless it is
                                % reactivated in the middle, and would stop without saving
                                log(info, "successful"),
                                {ok, #{}};
                            {error, Error} ->
                                log(info, "error deleting active actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end;
                    false ->
                        log(info, "calling actor_db_delete"),
                        case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, ActorId2, Opts]) of
                            {ok, DeleteMeta} ->
                                % In this case, we must send the deleted events
                                FakeActor = make_fake_actor(ActorId2),
                                nkactor_lib:send_external_event(SrvId, deleted, FakeActor),
                                log(info, "successful"),
                                {ok, DeleteMeta};
                            {error, Error} ->
                                log(info, "error deleting inactive actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end
                end;
            {error, Error} ->
                % We didn't identify any service
                %span_delete(delete),
                {error, Error}
        end
    end,
    nkserver_trace:new(same, "ActorBackend::delete", Fun, #{}).

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
    Fun = fun() ->
        log(info, "calling actor_db_search (~p) (~p)", [SearchType, Opts]),
        case ?CALL_SRV(SrvId, actor_db_search, [SrvId, SearchType, Opts]) of
            {ok, Actors, Meta} ->
                log(info, "search success: ~p", [Meta]),
                case parse_actors(SrvId, Actors, Opts) of
                    {ok, Actors2} ->
                        {ok, Actors2, Meta};
                    {error, Error} ->
                        {error, Error}
                end;
            {error, Error} ->
                log(info, "error in search: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new(SrvId, "ActorBackend::search", Fun).


%% @doc
-spec aggregation(nkactor:id(), agg_type(), map()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

aggregation(SrvId, AggType, Opts) ->
    Fun = fun() ->
        log(info, "agg type: ~p (~p)", [AggType, Opts]),
        log(info, "calling actor_db_aggregate"),
        case ?CALL_SRV(SrvId, actor_db_aggregate, [SrvId, AggType, Opts]) of
            {ok, Result, Meta} ->
                log(info, "success: ~p", [Meta]),
                {ok, Result, Meta};
            {error, Error} ->
                log(info, "error in agg: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new(SrvId, "ActorBackend::aggregate", Fun).


%% @doc
-spec truncate(nkactor:id(), map()) ->
    ok | {error, term()}.

truncate(SrvId, Opts) ->
    Fun = fun() ->
        log(info, "truncate (~p)", [Opts]),
        log(info, "calling actor_db_truncate"),
        case ?CALL_SRV(SrvId, actor_db_truncate, [SrvId, Opts]) of
            ok ->
                log(info, "success"),
                ok;
            {error, Error} ->
                log(info, "error in trunctae: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new(SrvId, "ActorBackend::truncate", Fun).


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
do_read(SrvId, ActorId, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    log(info, "calling actor_db_read"),
    case ?CALL_SRV(SrvId, actor_db_read, [SrvId, ActorId, Opts]) of
        {ok, Actor, Meta} ->
            % Actor's generic syntax is already parsed
            % Now we check specific syntax
            % If request option is provided, it is used for parsing
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            Req3 = maps:merge(#{ot_span_id=>SpanId}, Req2),
            log(info, "calling actor parse"),
            case Opts of
                #{no_data_parse:=true} ->
                    {ok, Actor, Meta};
                _ ->
                    case nkactor_actor:parse(SrvId, read, Actor, Req3) of
                        {ok, Actor2} ->
                            log(info, "actor is valid"),
                            {ok, Actor2, Meta};
                        {error, Error} ->
                            log(info, "error parsing actor: ~p", [Error]),
                            {error, Error}
                    end
            end;
        {error, Error} ->
            log(info, "error reading actor: ~p", [Error]),
            {error, Error}
    end.


%% @private
do_activate(Id, Opts, Tries) when Tries > 0 ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case find(Id, Opts) of
        {ok, SrvId, #actor_id{pid=Pid}=ActorId, Meta} when is_pid(Pid) ->
            {ok, SrvId, ActorId, Meta};
        {ok, SrvId, ActorId, _Meta} ->
            case do_read(SrvId, ActorId, Opts) of
                {ok, Actor, Meta2} ->
                    log(info, "calling actor_activate"),
                    Config1 = maps:with([ttl], Opts),
                    Config2 = Config1#{ot_span_id=>SpanId},
                    case ?CALL_SRV(SrvId, actor_activate, [Actor, Config2]) of
                        {ok, Pid} ->
                            log(info, "actor is activated"),
                            {ok, SrvId, ActorId#actor_id{pid=Pid}, Meta2};
                        {error, actor_already_activated} ->
                            lager:info("Already activated ~p: retrying (~p tries left)", [Id, Tries]),
                            timer:sleep(100),
                            do_activate(Id, Opts, Tries-1);
                        {error, Error} ->
                            log(notice, "error activating actor: ~p", [Error]),
                            {error, Error}
                    end;
                {error, persistence_not_defined} ->
                    log(info, "error activating actor: actor_not_found"),
                    {error, actor_not_found};
                {error, Error} ->
                    log(notice, "error activating actor: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end;

do_activate(Id, _Opts, _Tries) ->
    lager:notice("Error activating ~p: too_many_retries", [Id]),
    {error, actor_already_activated}.




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
    Fun = fun() ->
        search_activate_actors(SrvId, #{last_time=>Date, size=>PageSize}, 100, [])
    end,
    nkserver_trace:new(SrvId, "Actor::auto-activate", Fun).


%% @private
search_activate_actors(SrvId, Opts, Iters, Acc) when Iters > 0 ->
    log(info, "starting cursor: ~s", [maps:get(last_time, Opts, <<>>)]),
    ParentSpan = nkserver_ot:make_parent(?ACTIVATE_SPAN),
    Opts2 =  Opts#{ot_span_id=>ParentSpan},
    case search(SrvId, actors_activate, Opts2) of
        {ok, [], _} ->
            log(info, "no more actors"),
            {ok, lists:flatten(Acc)};
        {ok, ActorIds, #{last_time:=LastDate}} ->
            log(info, "found '~p' actors", [length(ActorIds)]),
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

parse_actors(SrvId, [#{data:=Data, metadata:=_Meta}=Actor|Rest], Req, Acc)
        when map_size(Data) > 0 ->
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
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            log(info, "actor parsed"),
            #{namespace:=Namespace} = Actor2,
            case nkactor_namespace:find_service(Namespace) of
                {ok, SrvId} ->
                    log(info, "actor namespace found: ~s", [SrvId]),
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
                                            log(info, "actor parsed"),
                                            {ok, SrvId, Actor6};
                                        {error, Error} ->
                                            log(notice, "error checking links: ~p", [Error]),
                                            {error, Error}
                                    end;
                                {error, Error} ->
                                    log(notice, "error parsing specific actor: ~p", [Error]),
                                    {error, Error}
                            end;
                        {error, Error} ->
                            log(info, "error creating initial data: ~p", [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    log(info, "error getting namespace: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            log(info, "error parsing generic actor: ~p", [Error]),
            {error, Error}
    end.


%% @private
pre_update(SrvId, ActorId, Actor, Opts) ->
    case nkactor_syntax:parse_actor(Actor, #{}) of
        {ok, Actor2} ->
            #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
            Base = #{group=>Group, resource=>Res, name=>Name, namespace=>Namespace},
            Actor3 = maps:merge(Base, Actor2),
            log(info, "actor parsed"),
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case nkactor_actor:parse(SrvId, update, Actor3, Req2) of
                {ok, Actor4} ->
                    case nkactor_lib:check_actor_links(Actor4) of
                        {ok, Actor5} ->
                            log(info, "actor parsed"),
                            {ok, Actor5};
                        {error, Error} ->
                            log(notice, "error checking links: ~p", [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    log(info, "error parsing specific actor: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            log(info, "error parsing generic actor: ~p", [Error]),
            {error, Error}
    end.


%%%% @private
%%span_create(Op, SrvId, Opts) ->
%%    Parent = maps:get(ot_span_id, Opts, undefined),
%%    Name = <<"ActorBackend::", (nklib_util:to_binary(Op))/binary>>,
%%    nkserver_ot:new(span_id(Op), SrvId, Name, Parent).
%%
%%
%%%% @private
%%span_id(Op) ->
%%    {?MODULE, Op}.
%%
%%
%%%% @private
%%span_finish(Op) ->
%%    nkserver_ot:finish(span_id(Op)).
%%
%%
%%%% @private
%%span_log(Op, Log) ->
%%    nkserver_ot:log(span_id(Op), Log).
%%
%%
%%%% @private
%%span_log(Op, Txt, Data) ->
%%    nkserver_ot:log(span_id(Op), Txt, Data).
%%
%%
%%%% @private
%%span_tags(Op, Tags) ->
%%    nkserver_ot:tags(span_id(Op), Tags).
%%
%%
%%%% @private
%%span_error(Op, Error) ->
%%    nkserver_ot:tag_error(span_id(Op), Error).
%%
%%
%%%% @private
%%span_update_srv_id(Op, SrvId) ->
%%    nkserver_ot:update_srv_id(span_id(Op), SrvId).
%%
%%
%%%% @private
%%span_delete(Op) ->
%%    nkserver_ot:delete(span_id(Op)).
