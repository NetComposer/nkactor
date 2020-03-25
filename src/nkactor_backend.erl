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
-import(nkserver_trace, [event/2, log/2, log/3, trace/1, trace/2]).

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

-spec find(nkactor:id(), map()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

find({OptSrvId, Id}, _Opts) when is_atom(OptSrvId) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    % Traces will be sent on the current span (lager:debug will be used if none)
    case nkactor_namespace:find_actor(ActorId) of
        {true, SrvId, #actor_id{pid=Pid}=ActorId2} when is_pid(Pid) ->
            % It is registered or cached
            log(debug, "actor is registered or cached: ~p (~s)", [Pid, SrvId]),
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
    trace("calling actor_db_find for ~p (~s)", [ActorId, SrvId]),
    case ?CALL_SRV(SrvId, actor_db_find, [SrvId, ActorId, #{}]) of
        {ok, #actor_id{} = ActorId2, Meta} ->
            % If its was an UID, or a partial path, we must check now that
            % it could be registered, now that we have full data
            case nkactor_namespace:find_actor(ActorId2) of
                {true, _SrvId, #actor_id{pid = Pid} = ActorId3} when is_pid(Pid) ->
                    log(debug, "actor found in disk and memory: ~p", [ActorId3]),
                    {ok, SrvId, ActorId3, Meta};
                _ ->
                    log(debug, "actor found in disk: ~p", [ActorId2] ),
                    {ok, SrvId, ActorId2, Meta}
            end;
        {error, actor_not_found} ->
            trace("actor not found in disk"),
            do_find(Rest, ActorId);
        {error, Error} ->
            log(notice, "error calling actor_db_find: ~p", [Error]),
            {error, Error}
    end.



%% @doc Finds an actors's pid or loads it from storage and activates it
%% See description for get_opts()
-spec activate(nkactor:id(), nkactor:get_opts()) ->
    {ok, nkserver:id(), #actor_id{}, Meta::map()} | {error, actor_not_found|term()}.

activate(Id, Opts) ->
    case do_activate(Id, Opts, 3) of
        {ok, SrvId, ActorId2, Meta2} ->
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
    case do_activate(Id, Opts, 3) of
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
    case pre_parse(Actor) of
        {ok, SrvId, Actor2} ->
            Fun = fun() ->
                case pre_create(SrvId, Actor2, Opts) of
                    {ok, Actor3} ->
                        #{
                            namespace := Namespace,
                            group := Group,
                            resource := Res,
                            name := Name,
                            uid := UID
                        } = Actor3,
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
                        trace("calling actor_db_create"),
                        case ?CALL_SRV(SrvId, actor_db_create, [SrvId, Actor3, Config]) of
                            {ok, Meta} ->
                                % Use the alternative method for sending the event
                                nkactor_lib:send_external_event(SrvId, created, Actor3),
                                case Opts of
                                    #{get_actor:=true} ->
                                        {ok, SrvId, Actor3, Meta};
                                    _ ->
                                        ActorId = nkactor_lib:actor_to_actor_id(Actor3),
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
            nkserver_trace:new_span(SrvId, {trace_nkactor_backend, create}, Fun);
        {error, Error} ->
            {error, Error}
    end;

create(Actor, Opts) ->
    case pre_parse(Actor) of
        {ok, SrvId, Actor2} ->
            Fun = fun() ->
                case pre_create(SrvId, Actor2, Opts) of
                    {ok, Actor3} ->
                        % If we use the activate option, the object is first
                        % registered with leader, so you cannot have two with same
                        % name even on non-relational databases
                        % The process will send the 'create' event in-server
                        Config = maps:with([ttl, no_unique_check], Opts),
                        trace("calling actor create"),
                        lager:error("NKLOG CALL CREATE"),
                        case ?CALL_SRV(SrvId, actor_create, [Actor3, Config]) of
                            {ok, Pid} when is_pid(Pid) ->
                                ActorId = nkactor_lib:actor_to_actor_id(Actor3),
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
                                        trace("calling get_actor"),
                                        case nkactor:sync_op(ActorId, get_actor, infinity) of
                                            {ok, Actor4} ->
                                                {ok, SrvId, Actor4, #{}};
                                            {error, actor_not_found} ->
                                                {error, actor_not_found};
                                            {error, Error} ->
                                                log(notice, "error getting actor: ~p", [Error]),
                                                nkserver_trace:error(Error),
                                                {error, Error}
                                        end;
                                    _ ->
                                        {ok, SrvId, ActorId, #{}}
                                end;
                            {error, Error} ->
                                log(notice, "error creating actor: ~p", [Error]),
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
            nkserver_trace:new_span(SrvId, {trace_nkactor_backend, create}, Fun);
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
    case do_activate(Id, Opts, 3) of
        {ok, SrvId, ActorId, _} ->
            Fun = fun() ->
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
                trace("calling update actor"),
                case pre_update(SrvId, ActorId, Actor, Opts) of
                    {ok, Actor2} ->
                        Opts2 = Opts#{parent => nkserver_trace:span_parent()},
                        case nkactor:sync_op(ActorId, {update, Actor2, Opts2}, infinity) of
                            {ok, Actor3} ->
                                {ok, SrvId, Actor3, #{}};
                            {error, Error} ->
                                log(notice, "error calling actor update actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end;
                    {error, Error} ->
                        log(notice, "error updating actor: ~p", [Error]),
                        nkserver_trace:error(Error),
                        {error, Error}
                end
            end,
            nkserver_trace:new_span(SrvId, {trace_nkactor_backend, update}, Fun);
        {error, Error} ->
            {error, Error}
    end.



%% @doc Deletes an actor
-spec delete(nkactor:id(), #{cascade=>boolean()}) ->
    {ok, map()} | {error, actor_not_found|term()}.

delete(Id, Opts) ->
    case find(Id, Opts) of
        {ok, SrvId, #actor_id{uid=UID, pid=Pid}=ActorId2, _Meta} ->
            Fun = fun() ->
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
                        trace("calling actor delete"),
                        case nkactor:sync_op(ActorId2, {delete, Opts}, infinity) of
                            ok ->
                                % The object is loaded, and it will perform the delete
                                % itself, including sending the event (a full event)
                                % It will stop, and when the backend calls raw_stop/2
                                % the actor would not be activated, unless it is
                                % reactivated in the middle, and would stop without saving
                                log(debug, "successful"),
                                {ok, #{}};
                            {error, Error} ->
                                log(notice, "error deleting active actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end;
                    false ->
                        trace("calling actor_db_delete"),
                        case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, ActorId2, Opts]) of
                            {ok, DeleteMeta} ->
                                % In this case, we must send the deleted events
                                FakeActor = make_fake_actor(ActorId2),
                                nkactor_lib:send_external_event(SrvId, deleted, FakeActor),
                                log(debug, "successful"),
                                {ok, DeleteMeta};
                            {error, Error} ->
                                log(notice, "error deleting inactive actor: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {error, Error}
                        end
                end
            end,
            nkserver_trace:new_span(SrvId, {trace_nkactor_backend, delete}, Fun);
        {error, Error} ->
            {error, Error}
    end.

%% @doc
delete_multi(SrvId, ActorIds) ->
    ?CALL_SRV(SrvId, actor_db_delete_multi, [SrvId, ActorIds, #{}]).



%% @doc
-spec search(nkactor:id(), search_type(), map()) ->
    {ok, [search_obj()], Meta::map()} | {error, term()}.

search(SrvId, SearchType, Opts) ->
    Fun = fun() ->
        log(info, "calling actor_db_search (~p) (~p)", [SearchType, Opts]),
        case ?CALL_SRV(SrvId, actor_db_search, [SrvId, SearchType, Opts]) of
            {ok, Actors, Meta} ->
                log(debug, "search success"),
                case parse_actors(SrvId, Actors, Opts) of
                    {ok, Actors2} ->
                        {ok, Actors2, Meta};
                    {error, Error} ->
                        {error, Error}
                end;
            {error, Error} ->
                log(notice, "error in search: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new_span(SrvId, {trace_nkactor_backend, search}, Fun).


%% @doc
-spec aggregation(nkactor:id(), agg_type(), map()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

aggregation(SrvId, AggType, Opts) ->
    Fun = fun() ->
        trace("calling actor_db_aggregate (~p) (~p)", [AggType, Opts]),
        case ?CALL_SRV(SrvId, actor_db_aggregate, [SrvId, AggType, Opts]) of
            {ok, Result, Meta} ->
                log(debug, "aggregation success"),
                {ok, Result, Meta};
            {error, Error} ->
                log(notice, "error in agg: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new_span(SrvId, {trace_nkactor_backend, aggregate}, Fun).


%% @doc
-spec truncate(nkactor:id(), map()) ->
    ok | {error, term()}.

truncate(SrvId, Opts) ->
    Fun = fun() ->
        log(debug, "truncate (~p)", [Opts]),
        trace("calling actor_db_truncate"),
        case ?CALL_SRV(SrvId, actor_db_truncate, [SrvId, Opts]) of
            ok ->
                log(debug, "success"),
                ok;
            {error, Error} ->
                log(notice, "error in truncate: ~p", [Error]),
                nkserver_trace:error(Error),
                {error, Error}
        end
    end,
    nkserver_trace:new_span(SrvId, {trace_nkactor_backend, truncate}, Fun).


%% ===================================================================
%% Internal
%% ===================================================================

%% @private
do_read(SrvId, ActorId, Opts) ->
    trace("calling actor_db_read"),
    case ?CALL_SRV(SrvId, actor_db_read, [SrvId, ActorId, Opts]) of
        {ok, Actor, Meta} ->
            % Actor's generic syntax is already parsed
            % Now we check specific syntax
            % If request option is provided, it is used for parsing
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case Opts of
                #{no_data_parse:=true} ->
                    {ok, Actor, Meta};
                _ ->
                    case nkactor_actor:parse(SrvId, read, Actor, Req2) of
                        {ok, Actor2} ->
                            log(debug, "actor is valid"),
                            {ok, Actor2, Meta};
                        {error, Error} ->
                            log(notice, "error parsing actor: ~p", [Error]),
                            {error, Error}
                    end
            end;
        {error, Error} ->
            log(notice, "error reading actor: ~p", [Error]),
            {error, Error}
    end.


%% @private
do_activate(Id, Opts, Tries) when Tries > 0 ->
    case find(Id, Opts) of
        {ok, SrvId, #actor_id{pid=Pid}=ActorId, Meta} when is_pid(Pid) ->
            {ok, SrvId, ActorId, Meta};
        {ok, SrvId, ActorId, _Meta} ->
            case do_read(SrvId, ActorId, Opts) of
                {ok, Actor, Meta2} ->
                    trace("calling actor_activate"),
                    Config = maps:with([ttl], Opts),
                    case ?CALL_SRV(SrvId, actor_activate, [Actor, Config]) of
                        {ok, Pid} ->
                            log(debug, "actor is activated"),
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
                    log(debug, "persistence_not_defined"),
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
    nkserver_trace:new_span(SrvId, {trace_nkactor_backend, auto_activate}, Fun).



%% @private
search_activate_actors(SrvId, Opts, Iters, Acc) when Iters > 0 ->
    log(info, "starting cursor: ~s", [maps:get(last_time, Opts, <<>>)]),
    case search(SrvId, actors_activate, Opts) of
        {ok, [], _} ->
            log(debug, "no more actors"),
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




pre_parse(Actor) ->
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            trace("actor parsed"),
            #{namespace:=Namespace} = Actor2,
            case nkactor_namespace:find_service(Namespace) of
                {ok, SrvId} ->
                    {ok, SrvId, Actor2};
                {error, Error} ->
                    log(notice, "error getting service for ~s: ~p", [Namespace, Error]),
                    {error, Error}
            end;
        {error, Error} ->
            log(notice, "error parsing generic actor: ~p", [Error]),
            {error, Error}
    end.


%% @private
pre_create(SrvId, Actor, Opts) ->
    case nkactor_lib:add_creation_fields(SrvId, Actor) of
        {ok, Actor2} ->
            Actor3 = case Opts of
                #{forced_uid:=UID} ->
                    nomatch = binary:match(UID, <<".">>),
                    <<First, _/binary>> = UID,
                    true = (First /= $/),
                    Actor2#{uid := UID};
                _ ->
                    Actor2
            end,
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case nkactor_actor:parse(SrvId, create, Actor3, Req2) of
                {ok, Actor4} ->
                    trace("calling pre_create check links"),
                    case nkactor_lib:check_actor_links(Actor4) of
                        {ok, Actor5} ->
                            log(debug, "actor pre_created"),
                            {ok, Actor5};
                        {error, Error} ->
                            log(notice, "error checking links: ~p", [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    log(notice, "error parsing specific actor: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            log(notice, "error creating initial data: ~p", [Error]),
            {error, Error}
    end.


%% @private
pre_update(SrvId, ActorId, Actor, Opts) ->
    case nkactor_syntax:parse_actor(Actor, #{}) of
        {ok, Actor2} ->
            #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
            Base = #{group=>Group, resource=>Res, name=>Name, namespace=>Namespace},
            Actor3 = maps:merge(Base, Actor2),
            log(debug, "pre_update: actor parsed"),
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case nkactor_actor:parse(SrvId, update, Actor3, Req2) of
                {ok, Actor4} ->
                    trace("calling pre_update check links"),
                    case nkactor_lib:check_actor_links(Actor4) of
                        {ok, Actor5} ->
                            log(debug, "actor pre_updated"),
                            {ok, Actor5};
                        {error, Error} ->
                            log(notice, "error checking links: ~p", [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    log(notice, "error parsing specific actor: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            log(notice, "error parsing generic actor: ~p", [Error]),
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
