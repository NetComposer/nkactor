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
%% -export([check_service/4]).
%%-export_type([search_obj/0, search_objs_opts/0]).

-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").


-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR DB "++Txt, Args)).

-define(BACKEND_SPAN, nkactor_backend).

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
%% If 'ot_span_id' is defined, logs will be added and it will be used as parent

-spec find(nkactor:id(), map()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

find(Id, Opts) ->
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
        false ->
            SrvIds = nkactor_util:get_services(),
            do_find(SrvIds, ActorId, SpanId)
    end.


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

-spec activate(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

activate(Id, Opts) ->
    case find(Id, Opts) of
        {ok, SrvId, #actor_id{pid=Pid}=ActorId, Meta} when is_pid(Pid) ->
            {ok, SrvId, ActorId, Meta};
        {ok, SrvId, ActorId, _Meta} ->
            case do_read(SrvId, ActorId, Opts) of
                {ok, Actor, Meta2} ->
                    SpanId = maps:get(ot_span_id, Opts, undefined),
                    nkserver_ot:log(SpanId, <<"calling actor_activate">>),
                    Config = maps:with([ttl, ot_span_id], Opts),
                    case ?CALL_SRV(SrvId, actor_activate, [Actor, Config]) of
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
%%
%% See description for get_opts()

-spec read(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkactor:actor(),  Meta::map()} | {error, actor_not_found|term()}.

read(Id, #{activate:=false}=Opts) ->
    case maps:get(consume, Opts, false) of
        true ->
            {error, cannot_consume};
        false ->
            case find(Id, Opts) of
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
    SpanId = maps:get(ot_span_id, Opts, undefined),
    nkserver_ot:new(?BACKEND_SPAN, <<"Actor::create">>, SpanId),
    nkserver_ot:log(?BACKEND_SPAN, <<"starting actor create without activation">>),
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_util:pre_create(Actor, Syntax, Opts) of
        {ok, SrvId, Actor2} ->
            #{
                namespace := Namespace,
                group := Group,
                resource := Res,
                name := Name,
                uid := UID
            } = Actor2,
            nkserver_ot:tags(?BACKEND_SPAN, #{
                <<"namespace">> => Namespace,
                <<"group">> => Group,
                <<"resource">> => Res,
                <<"name">> => Name,
                <<"uid">> => UID
            }),
            % Non recommended for non-relational databases, if name is not
            % randomly generated
            Config1 = maps:with([check_unique], Opts),
            Config2 = Config1#{ot_span_id=>?BACKEND_SPAN},
            nkserver_ot:log(?BACKEND_SPAN, <<"calling actor_db_create">>),
            case ?CALL_SRV(SrvId, actor_db_create, [SrvId, Actor2, Config2]) of
                {ok, Meta} ->
                    % Use the alternative method for sending the event
                    nkactor_lib:send_external_event(SrvId, created, Actor2),
                    case Opts of
                        #{get_actor:=true} ->
                            ActorId = nkactor_lib:actor_to_actor_id(Actor2),
                            nkserver_ot:finish(?BACKEND_SPAN),
                            {ok, SrvId, ActorId, Meta};
                        _ ->
                            nkserver_ot:finish(?BACKEND_SPAN),
                            {ok, SrvId, Actor2, Meta}
                    end;
                {error, Error} ->
                    nkserver_ot:log(?BACKEND_SPAN, <<"error creating actor: ~p">>, [Error]),
                    nkserver_ot:tag_error(?BACKEND_SPAN, Error),
                    nkserver_ot:finish(?BACKEND_SPAN),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(?BACKEND_SPAN, <<"error creating actor: ~p">>, [Error]),
            nkserver_ot:tag_error(?BACKEND_SPAN, Error),
            nkserver_ot:finish(?BACKEND_SPAN),
            {error, Error}
    end;

create(Actor, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    nkserver_ot:new(?BACKEND_SPAN, <<"Actor::create">>, SpanId),
    nkserver_ot:log(?BACKEND_SPAN, <<"starting actor create">>),
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_util:pre_create(Actor, Syntax, Opts#{ot_span_id=>?BACKEND_SPAN}) of
        {ok, SrvId, Actor2} ->
            #{
                namespace := Namespace,
                group := Group,
                resource := Res,
                name := Name,
                uid := UID
            } = Actor2,
            nkserver_ot:tags(?BACKEND_SPAN, #{
                <<"namespace">> => Namespace,
                <<"group">> => Group,
                <<"resource">> => Res,
                <<"name">> => Name,
                <<"uid">> => UID
            }),
            % If we use the activate option, the object is first
            % registered with leader, so you cannot have two with same
            % name even on non-relational databases
            % The process will send the 'create' event in-server
            Config1 = maps:with([ttl, check_unique], Opts),
            Config2 = Config1#{ot_span_id=>?BACKEND_SPAN},
            nkserver_ot:log(?BACKEND_SPAN, <<"calling actor_create">>),
            case ?CALL_SRV(SrvId, actor_create, [Actor2, Config2]) of
                {ok, #actor_id{pid=Pid, uid=UID}=ActorId} when is_pid(Pid) ->
                    nkserver_ot:tags(?BACKEND_SPAN, #{
                        <<"actor.uid">> => UID,
                        <<"actor.pid">> => list_to_binary(pid_to_list(Pid))
                    }),
                    case Opts of
                        #{get_actor:=true} ->
                            nkserver_ot:log(?BACKEND_SPAN, <<"calling get_actor">>),
                            case nkactor_srv:sync_op(ActorId, get_actor, infinity) of
                                {ok, Actor3} ->
                                    nkserver_ot:finish(?BACKEND_SPAN),
                                    {ok, SrvId, Actor3, #{}};
                                {error, Error} ->
                                    nkserver_ot:log(?BACKEND_SPAN, <<"error getting actor: ~p">>, [Error]),
                                    nkserver_ot:tag_error(?BACKEND_SPAN, Error),
                                    nkserver_ot:finish(?BACKEND_SPAN),
                                    {error, Error}
                            end;
                        _ ->
                            nkserver_ot:finish(?BACKEND_SPAN),
                            {ok, SrvId, ActorId, #{}}
                    end;
                {error, actor_already_registered} ->
                    nkserver_ot:log(?BACKEND_SPAN, <<"uniquess violation">>),
                    nkserver_ot:finish(?BACKEND_SPAN),
                    {error, uniqueness_violation};
                {error, Error} ->
                    nkserver_ot:log(?BACKEND_SPAN, <<"error creating actor: ~p">>, [Error]),
                    nkserver_ot:tag_error(?BACKEND_SPAN, Error),
                    nkserver_ot:finish(?BACKEND_SPAN),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(?BACKEND_SPAN, <<"error creating actor: ~p">>, [Error]),
            nkserver_ot:tag_error(?BACKEND_SPAN, Error),
            nkserver_ot:finish(?BACKEND_SPAN),
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
    case find(Id, Opts) of
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


%% @doc
-spec truncate(nkactor:id(), map()) ->
    ok | {error, term()}.

truncate(SrvId, Opts) ->
    ?CALL_SRV(SrvId, actor_db_truncate, [SrvId, Opts]).


%% ===================================================================
%% Internal
%% ===================================================================

do_read(SrvId, ActorId, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    nkserver_ot:log(SpanId, <<"calling actor_db_read">>),
    case ?CALL_SRV(SrvId, actor_db_read, [SrvId, ActorId, Opts]) of
        {ok, Actor, Meta} ->
            % Actor's generic syntax is already parsed
            % Now we check specific syntax
            % If request option is provided, it is used for parsing
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{
                verb => get,
                srv => SrvId
            },
            Req3 = maps:merge(#{ot_span_id=>SpanId}, Req2),
            case nkactor_actor:parse(SrvId, Actor, Req3) of
                {ok, Actor2} ->
                    {ok, Actor2, Meta};
                {error, Error} ->
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
