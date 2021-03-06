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
-export([msg/1]).
-export([actor_authorize/1, actor_config/1, actor_create/2, actor_activate/2,
         actor_external_event/3]).
-export([actor_srv_init/2, actor_srv_terminate/2,
         actor_srv_stop/2, actor_srv_get/2, actor_srv_update/2, actor_srv_delete/1,
         actor_srv_event/2,
         actor_srv_link_event/4, actor_srv_link_down/3, actor_srv_save/2,
         actor_srv_sync_op/3, actor_srv_async_op/2,
         actor_srv_enabled/2, actor_srv_next_status_timer/1,
         actor_srv_alarms/1, actor_srv_heartbeat/1,
         actor_srv_handle_call/3, actor_srv_handle_cast/2, actor_srv_handle_info/2]).
-export([actor_do_active/1, actor_do_expired/1]).
-export([actor_db_find/3, actor_db_read/3, actor_db_create/3, actor_db_update/3,
         actor_db_delete/3, actor_db_search/3, actor_db_aggregate/3,
         actor_db_truncate/2]).
-export([srv_master_init/2, srv_master_handle_call/4, srv_master_handle_cast/3,
         srv_master_handle_info/3, srv_master_timed_check/3]).


-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").
-type continue() :: continue | {continue, list()}.


%% ===================================================================
%% Errors Callbacks
%% ===================================================================


msg(actor_deleted)                  -> "Actor has been deleted";
msg({actors_deleted, N})            -> {"Actors (~p) have been deleted", [N]};
msg(actor_already_registered)       -> "Actor already registered";
msg(actor_not_found)                -> "Actor not found";
msg({actor_invalid, _})             -> "Actor is invalid";
msg(actor_id_invalid)               -> "Actor ID is invalid";
msg(actor_expired)	                -> "Actor has expired";
msg(actor_updated)                  -> "Actor updated";
msg(actor_has_linked_actors)	    -> "Actor has linked actors";
msg(actor_is_not_activable)	        -> "Actor is not activable";
msg(auth_invalid) 	                -> "Auth token is not valid";
msg(cannot_consume)                 -> "Actor cannot be consumed";
msg(delete_too_deep)                -> "DELETE is too deep";
msg({namespace_not_found, N})       -> {"Namespace '~s' not found", [N]};
msg({namespace_invalid, N})         -> {"Namespace '~s' is invalid", [N]};
msg(_)   		                    -> continue.




%% ===================================================================
%% Actor callbacks
%% ===================================================================

-type actor() :: nkactor:actor().
-type actor_id() :: #actor_id{}.
-type actor_st() :: #actor_st{}.



%% @doc Called when activating an actor to get it's config and module
%% Config is the added config used when calling the function
-spec actor_authorize(nkactor:request()) ->
    {true, nkactor:request()} | false | continue().

actor_authorize(_Req) ->
    false.


%% @doc Called when activating an actor to get it's config and module
%% Config is the added config used when calling the function
-spec actor_config(nkactor:config()) ->
    nkactor:config().

actor_config(Config) ->
    Config.


%% @doc Called from nkactor_db:create() when an actor is to be created
%% Can be used to select a different node, etc()
%% By default we start it at this node
-spec actor_create(nkactor:actor(), nkactor:config()) ->
    {ok, actor_id()} | {error, term()}.

actor_create(Actor, Config) ->
    nkactor_srv:create(Actor, Config).


%% @doc Called from nkactor_db:load() when an actor has been read and must be activated
%% Can be used to select a different node, etc()
%% By default we start it at this node
-spec actor_activate(nkactor:actor(), nkactor:config()) ->
    {ok, actor_id()} | {error, term()}.

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
    {ok, actor_st()} | {error, nkactor_msg:msg(), actor_st()} | continue().

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
    {ok, nkactor:actor(), actor_st()} | {error, nkserver:msg(), actor_st()} |continue().

actor_srv_update(Actor, ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [update, Group, Res, [Actor, ActorSt]]) of
        continue ->
            {ok, Actor, ActorSt};
        Other ->
            Other
    end.


%%  @doc Called before finishing a deletion
-spec actor_srv_delete(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:msg(), actor_st()} |continue().

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
%% from nkactor_srv:do_event/2
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
%% from nkactor_srv:do_event_link/2
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
-spec actor_srv_next_status_timer(actor_st()) ->
    {ok, actor_st()} | continue().

actor_srv_next_status_timer(ActorSt) ->
    #actor_st{srv=SrvId, actor_id=#actor_id{group=Group, resource=Res}} = ActorSt,
    case ?CALL_SRV(SrvId, nkactor_callback, [next_status_timer, Group, Res, [ActorSt]]) of
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
-spec actor_srv_stop(nkserver:msg(), actor_st()) ->
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
-spec actor_do_active(nkactor_srv:actor()) ->
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

-type db_opts() :: map().

-type db_create_opts() ::
    #{
        parent_span => term(),
        check_unique => boolean()       % Default true
    }.




%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(nkserver:id(), actor_id(), db_opts()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_find(_SrvId, _ActorId, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must find and read a full actor on disk by UID (if available) or name
-spec actor_db_read(nkserver:id(), actor_id(), map()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_read(_SrvId, _ActorId, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(nkserver:id(), actor(), db_create_opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()}.

actor_db_create(_SrvId, _Actor, _Opts) ->
    {error, persistence_not_defined}.


%% @doc Must update a new actor on disk.
-spec actor_db_update(nkserver:id(), actor(), map()) ->
    {ok, Meta::map()} | {error, term()}.

actor_db_update(_SrvId, _Actor, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_delete(nkserver:id(), [nkactor:uid()], map()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()}.


actor_db_delete(_SrvId, _UIDs, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_search(nkserver:id(), nkactor_backend:search_type(), map()) ->
    term().

actor_db_search(_SrvId, _Type, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_aggregate(nkserver:id(), nkactor_backend:agg_type(), map()) ->
    term().

actor_db_aggregate(_SrvId, _Type, _Opts) ->
    {error, persistence_not_defined}.


%% @doc
-spec actor_db_truncate(nkserver:id(), map()) ->
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

