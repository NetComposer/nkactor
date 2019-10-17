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

%% @doc Default plugin callbacks
-module(nkactor_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([status/1]).
-export([actor_plugin_init/1, actor_req_authorize/1, actor_config/1,
         actor_fields_filter/1, actor_fields_sort/1,
         actor_fields_trans/1, actor_fields_type/1, actor_fields_static/1,
         actor_create/2, actor_activate/2, actor_external_event/3]).
-export([actor_path_to_id/2]).
-export([actor_srv_init/2, actor_srv_terminate/2,
         actor_srv_stop/2, actor_srv_get/2, actor_srv_update/2, actor_srv_delete/1,
         actor_srv_event/2,
         actor_srv_link_event/4, actor_srv_link_down/3, actor_srv_save/2,
         actor_srv_sync_op/3, actor_srv_async_op/2,
         actor_srv_enabled/2, actor_srv_activate_timer/2, actor_srv_expired/2,
         actor_srv_alarms/1, actor_srv_heartbeat/1,
         actor_srv_handle_call/3, actor_srv_handle_cast/2, actor_srv_handle_info/2]).
-export([actor_do_active/1, actor_do_expired/1]).
-export([actor_db_find/3, actor_db_read/3, actor_db_create/3, actor_db_update/3,
         actor_db_delete/3, actor_db_delete_multi/3, actor_db_search/3, actor_db_aggregate/3,
         actor_db_truncate/2]).
-export([srv_master_init/2, srv_master_handle_call/4, srv_master_handle_cast/3,
         srv_master_handle_info/3, srv_master_timed_check/3]).


-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").
-type continue() :: continue | {continue, list()}.


%% ===================================================================
%% Status Callbacks
%% ===================================================================


status(actor_already_registered)    -> "Actor already registered";
status(actor_deleted)               -> {"Actor has been deleted", #{code=>200}};
status({actors_deleted, N})         -> {"Actors (~p) have been deleted", [N]};
status({actor_invalid, A})          -> {"Invalid actor '~s'", [A], #{code=>400, data=>#{actor=>A}}};
status(actor_expired)	            -> {"Actor has expired", #{code=>404}};
status({actor_already_exists, U})	-> {"Actor '~s' already exists", [U], #{code=>409, data=>#{uid=>U}}};
status(actor_has_linked_actors)	    -> {"Actor has linked actors", #{code=>422}};
status(actor_id_invalid)            -> "Actor ID is invalid";
status(actor_is_not_activable)	    -> {"Actor is not activable", #{code=>422}};
status(actor_not_found)             -> {"Actor not found", #{code=>404}};
status(actor_updated)               -> {"Actor has been updated", #{code=>200}};
status({api_group_unknown, G})      -> {"Unknown API Group '~s'", [G], #{code=>404, data=>#{group=>G}}};
status({api_incompatible, A})       -> {"Incompatible API '~s'", [A], #{code=>422, data=>#{api=>A}}};
status({api_unknown, A})            -> {"Unknown API '~s'", [A], #{code=>404, data=>#{api=>A}}};
status({body_too_large, Max})       -> {"Body too large (max is ~p)", [Max]};
status(cannot_consume)              -> "Actor cannot be consumed";
status(data_value_invalid)          -> {"Data value is invalid", #{code=>400}};
status(db_not_defined)              -> "Object database not defined";
status(delete_too_deep)             -> "DELETE is too deep";
status(download_server_error)       -> "Download server error";
status(element_action_unknown)      -> "Unknown element action";
status({field_unknown, F})          -> {"Unknown field '~s'", [F], #{code=>400, data=>#{field=>F}}};
status(group_unknown)               -> "Invalid Group";
status(invalid_content_type)        -> "Invalid Content-Type";
status({invalid_name, N})           -> {"Invalid name '~s'", [N]};
status(invalid_session)             -> "Invalid session";
status(kind_unknown)                -> "Invalid kind";
status({kind_unknown, K})           -> {"Invalid kind '~s'", [K], #{code=>400, data=>#{kind=>K}}};
status(linked_actor_unknown)        -> {"Linked actor is unknown", #{code=>409}};
status({linked_actor_unknown, Id})  -> {"Linked actor is unknown: ~s", [Id], #{code=>409, data=>#{link=>Id}}};
status(multiple_ids)                -> "Multiple matching ids";
status(missing_auth_header)         -> "Missing authentication header";
status(namespace_missing)           -> {"Namespace is missing", #{code=>409}};
status({provider_unknown, P})       -> {"Provider unknown: '~s'", [P], #{code=>422, data=>#{provider=>P}}};
status({request_class_unknown, C})  -> {"Request class uknown: '~s'", [C]};
status({resource_invalid, R})       -> {"Invalid resource '~s'", [R], #{code=>400}};
status({resource_invalid, C, R})    -> {"Invalid resource '~s' (~s)", [R, C], #{code=>400}};
status(session_already_present)     -> "Session is already present";
status(session_not_found)           -> "Session not found";
status(session_is_disabled)         -> "Session is disabled";
status(ttl_missing)                 -> {"TTL is missing", #{code=>400}};
status(uid_not_found)      		    -> "Unknown UID";
status(uniqueness_violation)        -> {"Uniqueness violation", #{code=>409}};
status(upload_server_error)         -> "Upload server error";
status({updated_invalid_field, F})  -> {"Updated invalid field '~s'", [F], #{code=>422, data=>#{field=>F}}};
status({updated_static_field, F})  -> {"Updated static field '~s'", [F], #{code=>422, data=>#{field=>F}}};
status(user_unknown)                -> {"User unknown", #{code=>404}};
status({user_unknown, U})           -> {"User '~s' unknown", [U], #{code=>404, data=>#{user=>U}}};
status(url_unknown)      		    -> "Unknown url";
status(watch_stop)                  -> "Watch stopped";
status(_) -> continue.



%% ===================================================================
%% Actor callbacks
%% ===================================================================

-type actor() :: nkactor:actor().
-type actor_id() :: #actor_id{}.
-type actor_st() :: #actor_st{}.
%-type request() :: actor_request:request().


%% @doc Called when processing a request to be authorized or not
-spec actor_plugin_init(nkserver:id()) ->
    ok.

actor_plugin_init(_SrvId) ->
    ok.


%% @doc Called when processing a request to be authorized or not
-spec actor_req_authorize(nkactor:request()) ->
    {true, nkactor:request()} | false | continue().

actor_req_authorize(_Req) ->
    false.


%% @doc Called when activating an actor to get it's config and module.
%% Can be used to updated the config for an actor resource
-spec actor_config(nkactor:config()) ->
    nkactor:config().

actor_config(Config) ->
    Config.


%% @doc Called by config to get common filter fields for all actors
-spec actor_fields_filter([atom()]) ->
    [atom()].

actor_fields_filter(List) ->
    List ++ nkactor_syntax:actor_fields_filter().


%% @doc Called by config to get common sort fields for all actors
-spec actor_fields_sort([atom()]) ->
    [atom()].

actor_fields_sort(List) ->
    List ++ nkactor_syntax:actor_fields_sort().


%% @doc Called by config to get fields that should be translated to another field for all actors
-spec actor_fields_trans(#{atom() => atom()}) ->
    #{atom() => atom()}.

actor_fields_trans(Map) ->
    maps:merge(Map, nkactor_syntax:actor_fields_trans()).


%% @doc Called by config to get field's types for all actors
-spec actor_fields_type(#{atom() => integer|boolean|string}) ->
    #{atom() => integer|boolean|string}.

actor_fields_type(Map) ->
    maps:merge(Map, nkactor_syntax:actor_fields_type()).


%% @doc Called by config to get common sort fields for all actors
-spec actor_fields_static([atom()]) ->
    [atom()].

actor_fields_static(List) ->
    List ++ nkactor_syntax:actor_fields_static().


%% @doc Called when an external id must be decoded, starting with "/"
%% Service will be detected is [..., <<"namespaces">>, Namespace, ...] is present
%% Otherwise, first actor service will be called
-spec actor_path_to_id(nkserver:id(), [binary()]) ->
    #actor_id{} | continue.

actor_path_to_id(_SrvId, _Parts) ->
    continue.


%% @doc Called from nkactor_db:create() when an actor is to be created
%% Can be used to select a different node, etc()
%% By default we start it at this node
-spec actor_create(nkactor:actor(), nkactor:config()) ->
    {ok, pid()} | {error, term()}.

actor_create(Actor, Config) ->
    nkactor_srv:create(Actor, Config).


%% @doc Called from nkactor_backend:activate() when an actor has been read and must
%% be activated. Can be used to select a different node, etc.
%% By default we start it at this node
-spec actor_activate(nkactor:actor(), nkactor:config()) ->
    {ok, pid()} | {error, term()}.

actor_activate(Actor, Config) ->
    nkactor_srv:start(Actor, Config).


%% @doc Called from nkactor_lib:send_external_event/3 to send
%% created, deleted or updated operations on non-activated actors
-spec actor_external_event(nkactor:id(), created|deleted|updated, actor()) ->
    ok | continue.

actor_external_event(_SrvId, _Event, _Actor) ->
    ok.


%% @doc Called on successful registration (callback init/2 in actor)
-spec actor_srv_init(create|start, actor_st()) ->
    {ok, actor_st()} | {error, Reason::term()}.

actor_srv_init(Op, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [init, Group, Res, [Op, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called on a periodic basis, if heartbeat_time is defined  (callback heartbeat/1 in actor)
-spec actor_srv_heartbeat(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:status(), actor_st()} | continue().

actor_srv_heartbeat(ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [heartbeat, Group, Res, [ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%%  @doc Called when performing operations get_actor and consume_actor
%% to modify the returned actor  (callback get/2 in actor)
-spec actor_srv_get(nkactor:actor(), actor_st()) ->
    {ok, nkactor:actor(), actor_st()} | continue().

actor_srv_get(Actor, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [get, Group, Res, [Actor, ActorSt]]) of
        continue ->
            {ok, Actor, ActorSt};
        Other ->
            Other
    end.


%%  @doc Called after approving an update, to change the updated actor  (callback update/2 in actor)
-spec actor_srv_update(nkactor:actor(), actor_st()) ->
    {ok, nkactor:actor(), actor_st()} | {error, nkserver:status(), actor_st()} |continue().

actor_srv_update(Actor, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [update, Group, Res, [Actor, ActorSt]]) of
        continue ->
            {ok, Actor, ActorSt};
        {ok, Actor2, ActorSt2} ->
            {ok, Actor2, ActorSt2};
        {ok, #actor_st{actor=Actor2}=ActorSt2} ->
            {ok, Actor2, ActorSt2};
        Other ->
            Other
    end.


%%  @doc Called before finishing a deletion
-spec actor_srv_delete(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:status(), actor_st()} |continue().

actor_srv_delete(ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [delete, Group, Res, [ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when an object is enabled/disabled
-spec actor_srv_enabled(boolean(), actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_enabled(Enabled, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [enabled, Group, Res, [Enabled, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called to send an event from inside an actor's process
%% from nkactor_srv_lib:event/2
%% The events are 'erlang' events (tuples usually)
-spec actor_srv_event(term(), actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_event(Event, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [event, Group, Res, [Event, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when an event is sent, for each registered process to the session
%% from nkactor_srv_lib:event_link/2
%% The events are 'erlang' events (tuples usually)
-spec actor_srv_link_event(nklib:link(), term(), nkactor_srv:event(), actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_link_event(Link, LinkData, Event, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [link_event, Group, Res, [Link, LinkData, Event, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Allows to modify the actor that is going to be saved
-spec actor_srv_save(actor(), actor_st()) ->
    {ok, actor(), actor_st()} | {ignore, actor_st()} | continue().

actor_srv_save(Actor, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [save, Group, Res, [Actor, ActorSt]]) of
        {ok, Actor2, ActorSt2} ->
            {ok, Actor2, ActorSt2};
        {ok, #actor_st{actor=Actor2}=ActorSt2} ->
            {ok, Actor2, ActorSt2};
        continue ->
            {ok, Actor, ActorSt};
        Other ->
            Other
    end.


%% @doc
%% If you reply with reply, and is_dirty is set to true, it will start a save
%% timer if not in place yet
-spec actor_srv_sync_op(term(), {pid(), reference()}, actor_st()) ->
    {reply, Reply::term(), actor_st()} | {reply_and_save, Reply::term(), actor_st()} |
    {noreply, actor_st()} | {noreply_and_save, actor_st()} |
    {stop, Reason::term(), Reply::term(), actor_st()} |
    {stop, Reason::term(), actor_st()} |
    continue().

actor_srv_sync_op(Op, From, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    ?CALL_SRV(SrvId, nkactor_callback, [sync_op, Group, Res, [Op, From, ActorSt]]).


%% @doc
-spec actor_srv_async_op(term(), actor_st()) ->
    {noreply, actor_st()} | {noreply_and_save, actor_st()} |
    {stop, Reason::term(), actor_st()} |
    continue().

actor_srv_async_op(Op, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    ?CALL_SRV(SrvId, nkactor_callback, [async_op, Group, Res, [Op, ActorSt]]).


%% @doc Called when a linked process goes down
-spec actor_srv_link_down(nklib_links:link(), Data::term(), actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_link_down(Link, Data, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [link_down, Group, Res, [Link, Data, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when the timer in next_status_time is fired
-spec actor_srv_activate_timer(Time::binary(), actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_activate_timer(Time, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [activated, Group, Res, [Time, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when the actor is expired
-spec actor_srv_expired(Time::binary(), actor_st()) ->
    {ok, actor_st()} | {delete, actor_st()} | continue().

actor_srv_expired(Time, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [expired, Group, Res, [Time, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when a object with alarms is loaded
-spec actor_srv_alarms(actor_st()) ->
    {ok, actor_st()} | {error, term(), actor_st()} | continue().

actor_srv_alarms(ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [alarms, Group, Res, [ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc
-spec actor_srv_handle_call(term(), {pid(), term()}, actor_st()) ->
    {reply, term(), actor_st()} | {noreply, actor_st()} |
    {stop, Reason::term(), Reply::term(), actor_st()} |
    {stop, Reason::term(), actor_st()} | continue().

actor_srv_handle_call(Msg, From, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [handle_call, Group, Res, [Msg, From, ActorSt]]) of
        continue ->
            lager:error("Module nkactor_srv received unexpected call: ~p", [Msg]),
            {noreply, ActorSt};
        Other ->
            Other
    end.


%% @doc
-spec actor_srv_handle_cast(term(), actor_st()) ->
    {noreply, actor_st()} | {stop, term(), actor_st()} | continue().

actor_srv_handle_cast(Msg, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [handle_cast, Group, Res, [Msg, ActorSt]]) of
        continue ->
            lager:error("Module nkactor_srv received unexpected cast: ~p", [Msg]),
            {noreply, ActorSt};
        Other ->
            Other
    end.


%% @doc
-spec actor_srv_handle_info(term(), actor_st()) ->
    {noreply, actor_st()} | {stop, term(), actor_st()} | continue().

actor_srv_handle_info(Msg, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [handle_info, Group, Res, [Msg, ActorSt]]) of
        continue ->
            lager:error("Module nkactor_srv received unexpected info: ~p", [Msg]),
            {noreply, ActorSt};
        Other ->
            Other
    end.


%% @private Called on proper stop
-spec actor_srv_stop(nkserver:status(), actor_st()) ->
    {ok, actor_st()} | {delete, actor_st()} | continue().

actor_srv_stop(Reason, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [stop, Group, Res, [Reason, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.


%% @doc Called when the server terminate is called
-spec actor_srv_terminate(Reason::term(), actor_st()) ->
    {ok, actor_st()}.

actor_srv_terminate(Reason, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [terminate, Group, Res, [Reason, ActorSt]]) of
        continue ->
            {ok, ActorSt};
        Other ->
            Other
    end.



%% ===================================================================
%% Actor utilities
%% ===================================================================

%% @doc Called when an 'isActivated' actor is read
%% If 'removed' is returned the actor will not load
-spec actor_do_active(nkactor:actor()) ->
    ok | removed.

actor_do_active(_Actor) ->
    ok.


%% @doc Called when an expired actor is read
-spec actor_do_expired(nkactor:actor()) ->
    ok.

actor_do_expired(_Actor) ->
    ok.


%%%% @doc
%%-spec actor_db_get_service(nkactor:id(), nkactor:id()) ->
%%    {ok, nkactor_db:service_info()} | {error, term()}.
%%
%%actor_db_get_service(_SrvId, _ActorSrvId) ->
%%    {error, actor_db_not_implemented}.
%%
%%
%%%% @doc
%%-spec actor_db_update_service(nkactor:id(), nkactor:id(), binary()) ->
%%    {ok, Meta::map()} | {error, term()}.
%%
%%actor_db_update_service(_SrvId, _ActorSrvId, _Cluster) ->
%%    {error, actor_db_not_implemented}.



%% ===================================================================
%% Persistence callbacks
%% ===================================================================

-type db_opts() ::
    #{
        % If ot_span_id is defined, it will be used as parent for new created spans
        ot_span_id => nkserver_ot:span_id() | nkserver_ot:parent(),
        % Default true
        no_unique_check => boolean(),
        % For deletions, default false
        cascade => boolean()
    }.




%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(nkserver:id(), actor_id(), db_opts()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_find(_SrvId, _ActorId, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must find and read a full actor on disk by UID (if available) or name
%% It MUST call nkactor_syntax:parse_actor/1 to get a valid actor
-spec actor_db_read(nkserver:id(), actor_id(), db_opts()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_read(_SrvId, _ActorId, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(nkserver:id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|field_invalid|term()}.

actor_db_create(_SrvId, _Actor, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must update a new actor on disk.
-spec actor_db_update(nkserver:id(), actor(), db_opts()) ->
    {ok, Meta::map()} | {error, term()}.

actor_db_update(_SrvId, _Actor, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_delete(nkserver:id(), actor_id(), db_opts()) ->
    {ok, Meta::map()} | {error, term()}.


actor_db_delete(_SrvId, _UID, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_delete_multi(nkserver:id(), [actor_id()], db_opts()) ->
    {ok, #{deleted:=integer()}} | {error, term()}.

actor_db_delete_multi(_SrvId, _UIDs, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_search(nkserver:id(), nkactor_backend:search_type(), db_opts()) ->
    term().

actor_db_search(_SrvId, _Type, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_aggregate(nkserver:id(), nkactor_backend:agg_type(), db_opts()) ->
    term().

actor_db_aggregate(_SrvId, _Type, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_truncate(nkserver:id(), db_opts()) ->
    term().

actor_db_truncate(_SrvId, _Opts) ->
    {error, persistence_not_defined}.



%% ===================================================================
%% Master callbacks
%% ===================================================================


%% @private
srv_master_init(SrvId, State) ->
    nkactor_master:srv_master_init(SrvId, State).


%% @private
srv_master_handle_call(Msg, From, SrvId, State) ->
    nkactor_master:srv_master_handle_call(Msg, From, SrvId, State).


%% @private
srv_master_handle_cast(Msg, SrvId, State) ->
    nkactor_master:srv_master_handle_cast(Msg, SrvId, State).


%% @private
srv_master_handle_info(Msg, SrvId, State) ->
    nkactor_master:srv_master_handle_info(Msg, SrvId, State).


%% @private
srv_master_timed_check(IsMaster, SrvId, State) ->
    nkactor_master:srv_master_timed_check(IsMaster, SrvId, State).

