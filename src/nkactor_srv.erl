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

%% @doc Basic Actor behaviour
%%
%% When the actor starts, it must register with its service's leader or it won't start
%% If the leader fails, the actor will save and unload immediately
%% (the leader keeps the registration for the actor, so it cannot be found any more)
%%
%% Unload policy
%%
%% - Permanent mode
%%      - if object has permanent => true in config() it is not unloaded
%%      - if object has auto_activate in config() or in metadata, it is not unloaded
%% - TTL mode
%%      otherwise, property default_ttl in object_info() is used for ttl, default is ?DEFAULT_TTL
%%      once expired, the object is unloaded if no childs or usages
%%      some functions restart de count calling do_refresh()



%%
%% Enabled policy
%%
%% - An actor starts disabled if property "isEnabled" is set to false
%% - All standard operations work normally (but see dont_update_on_disabled and dont_delete_on_disabled)
%% - When the actor is enabled or disabled, and event will be fired (useful for links)
%% - The behaviour will be dependant for each implementation
%%
%% Save policy
%%
%% - It will saved on 'save' operations (sync and async) that return
%%   reply_and_save and noreply_and_save on operations
%% - Also before deletion, on unload, on update (if is_dirty:true)
%% - After each sync or async operation, if 'is_dirty' is true, a timer is started
%%   (if it was not yet started) to save the actor automatically
%%
%% Opentracing
%%
%% - Some operations (like init and update) accept a parameter 'ot_span_id', and,
%%   if provided, will create a new span based on it
%% - Other operations (save) always generate a new span, based on previous defined
%%   operation span (from init or update) or top-level if none defined


-module(nkactor_srv).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-behaviour(gen_server).

-export([create/2, start/2, sync_op/2, sync_op/3, async_op/2, live_async_op/2, delayed_async_op/3, hibernate/0]).
-export([init/1, terminate/2, code_change/3, handle_call/3, handle_cast/2, handle_info/2]).
-export([count/0, get_state/1, get_timers/1, raw_stop/2]).
-export_type([event/0, save_reason/0]).

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver_ot/include/nkserver_ot.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%%-define(DEFAULT_TTL, 10000).
-define(DEF_SYNC_CALL, 5000).
-define(HEARTBEAT_TIME, 5000).
-define(DEFAULT_SAVE_TIME, 5000).
-define(MAX_STATUS_TIME, 24 * 60 * 60 * 1000).
%%-define(SPAN_MAIN, nkactor_span).
%%-define(SPAN_INIT, nkactor_span_init).
%%-define(SPAN_UPDATE, nkactor_span_update).
%%-define(SPAN_SAVE, nkactor_span_save).


%% ===================================================================
%% Types
%% ===================================================================


-type event() ::
    created |
    activated |
    saved |
    {updated, nkactor:actor()} |
    deleted |
    {enabled, boolean()} |
    {info, binary(), Meta :: map()} |
    {link_added, nklib_links:link()} |
    {link_removed, nklib_links:link()} |
    {link_down, Type :: binary()} |
    {alarm_fired, nkactor:alarm()} |
    alarms_all_cleared |
    {stopped, nkserver:status()}.


-type link_opts() ::
    #{
        get_events => boolean(),        % calls to actor_srv_link_event for each event (false)
        gen_events => boolean(),        % generate events {link_XXX} (true)
        avoid_unload => boolean(),      % do not unload until is unlinked (false)
        data => term()                  % user config
    }.


-type sync_op() ::
    get_actor |
    consume_actor |
    get_actor_id |
    get_links |
    save |
    {add_label, binary(), binary()} |
    delete |
    {enable, boolean()} |
    is_enabled |
    {link, nklib:link(), link_opts()} |
    {update, nkactor:actor(), nkactor:update_opts()} |
    {update_name, binary()} |
    get_alarms |
    {set_alarm, nkactor:alarm()} |
    {apply, Mod :: module(), Fun :: atom(), Args :: list()} |
    term().

-type async_op() ::
    {unlink, nklib:link()} |
    {send_info, atom()|binary(), map()} |
    {send_event, event()} |
    {set_activate_time, binary()|integer()} |
    {set_alarm, nkactor:alarm()} |
    clear_all_alarms |
    save |
    delete |
    {stop, Reason :: nkserver:status()} |
    term().


-type state() :: #actor_st{}.


-type save_reason() ::
creation | user_op | user_order | unloaded | update | timer.


-type start_opts() ::
    #{
        ttl => integer(),           % msecs
        ot_span_id => nkserver_ot:span_id() | nkserver_ot:parent()
    }
    |
    nkactor:config().





%% ===================================================================
%% Public
%% ===================================================================

%% @private
%% Call nkactor:create/2 instead calling this directly!
-spec create(nkactor:actor(), start_opts()) ->
    {ok, pid()} | {error, term()}.

create(Actor, Config) ->
    do_start(create, Actor, Config).


%% @private
%% Call nkactor:activate/1 instead calling this directly!
-spec start(nkactor:actor(), start_opts()) ->
    {ok, pid()} | {error, term()}.

start(Actor, Config) ->
    do_start(start, Actor, Config).


%% @private
do_start(Op, Actor, StartConfig) ->
    StartConfig2 = case StartConfig of
        #{ot_span_id:=SpanId} ->
            % We are going to a different process, if it is a
            % process dictionary span is not going to pass
            StartConfig#{ot_span_id:=nkserver_ot:make_parent(SpanId)};
        _ ->
            StartConfig
    end,
    Ref = make_ref(),
    Opts = {Op, Actor, StartConfig2, self(), Ref},
    case gen_server:start(?MODULE, Opts, []) of
        {ok, Pid} ->
            {ok, Pid};
        ignore ->
            receive
                {do_start_ignore, Ref, Result} ->
                    Result
            after 5000 ->
                error(do_start_ignore)
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec sync_op(nkactor:id()|pid(), sync_op()) ->
    term() | {error, timeout|process_not_found|actor_not_found|term()}.

sync_op(Id, Op) ->
    sync_op(Id, Op, ?DEF_SYNC_CALL).


%% @doc
-spec sync_op(nkactor:id()|pid(), sync_op(), timeout()) ->
    term() | {error, timeout|process_not_found|actor_not_found|term()}.

sync_op(Pid, Op, Timeout) when is_pid(Pid) ->
    case nklib_util:call2(Pid, {nkactor_sync_op, Op}, Timeout) of
        process_not_found ->
            {error, process_not_found};
        Other ->
            Other
    end;

sync_op(Id, Op, Timeout) ->
    sync_op(Id, Op, Timeout, 5).


%% @private
sync_op(Id, Op, _Timeout, 0) ->
    lager:warning("NkACTOR SynOP failed (too many retries) (~p) (~p)", [Id, Op]),
    {error, process_not_found};

sync_op(#actor_id{pid=Pid}=ActorId, Op, Timeout, Tries) when is_pid(Pid) ->
    case sync_op(Pid, Op, Timeout) of
        {error, process_not_found} ->
            ActorId2 = ActorId#actor_id{pid = undefined},
            timer:sleep(250),
            lager:notice("NkACTOR SynOP failed (~p), retrying...", [ActorId2]),
            sync_op(ActorId2, Op, Timeout, Tries - 1);
        Other ->
            Other
    end;

sync_op(Id, Op, Timeout, Tries) ->
    case nkactor:activate(Id) of
        {ok, #actor_id{pid = Pid} = ActorId2} when is_pid(Pid) ->
            sync_op(ActorId2, Op, Timeout, Tries);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec async_op(nkactor:id()|pid(), async_op()) ->
    ok | {error, process_not_found|actor_not_found|term()}.

async_op(Pid, Op) when is_pid(Pid) ->
    gen_server:cast(Pid, {nkactor_async_op, Op});

async_op(Id, Op) ->
    case nkactor:activate(Id) of
        {ok, #actor_id{pid = Pid}} when is_pid(Pid) ->
            async_op(Pid, Op);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec live_async_op(nkactor:id()|pid(), async_op()) ->
    ok | {error, term()}.

live_async_op(Pid, Op) when is_pid(Pid) ->
    async_op(Pid, {nkactor_async_op, Op});

live_async_op(Id, Op) ->
    case nkactor:find(Id) of
        {ok, #actor_id{pid = Pid}} when is_pid(Pid) ->
            async_op(Pid, Op);
        _ ->
            ok
    end.


%% @doc
-spec delayed_async_op(nkactor:id()|pid(), async_op(), pos_integer()) ->
    ok | {error, process_not_found|actor_not_found|term()}.

delayed_async_op(Pid, Op, Time) when is_pid(Pid), Time > 0 ->
    _ = spawn(
        fun() ->
            timer:sleep(Time),
            async_op(Pid, Op)
        end),
    ok;

delayed_async_op(Id, Op, Time) ->
    case nkactor:activate(Id) of
        {ok, #actor_id{pid = Pid}} when is_pid(Pid) ->
            delayed_async_op(Pid, Op, Time);
        {error, Error} ->
            {error, Error}
    end.


%% @private
%% If the object is activated, it will be unloaded without saving
%% before launching the deletion
raw_stop(Id, Reason) ->
    case nkactor_backend:find(Id, #{}) of
        {ok, _SrvId, #actor_id{pid = Pid}, _} when is_pid(Pid) ->
            async_op(Pid, {raw_stop, Reason});
        {error, Error} ->
            {error, Error};
        false ->
            ok
    end.


%% @private
hibernate() ->
    gen_server:cast(self(), nkactor_hibernate).


%% @private
get_state(Id) ->
    sync_op(Id, get_state).


%% @private
get_timers(Id) ->
    sync_op(Id, get_timers).


%% @doc
-spec count() -> integer().

count() ->
    nklib_counters:value(?MODULE).


% ===================================================================
%% gen_server behaviour
%% ===================================================================


%% @private
-spec init(term()) ->
    {ok, state()} | {stop, term()}.

init({Op, Actor, StartConfig, Caller, Ref}) ->
    % We create a top-level span for generic actor operations
    % Also, if Config has option 'ot_span_id' a specific operation span will be created
    % in do_pre_init
    ActorId = nkactor_lib:actor_to_actor_id(Actor),
    case nkactor_actor:get_config(ActorId) of
        {ok, _SrvId, #{activable:=false}} ->
            Caller ! {do_start_ignore, Ref, {error, actor_is_not_activable}},
            ignore;
        {ok, SrvId, BaseConfig} ->
            Config = maps:merge(BaseConfig, StartConfig),
            State1 = do_pre_init(ActorId, Actor, Config, SrvId),
            State2 = nkactor_srv_lib:op_span_check_create(Op, Config, State1),
            nkactor_srv_lib:op_span_log(<<"starting initialization: ~p">>, [Op], State2),
            #actor_id{
                group = Group,
                resource = Res,
                name = Name,
                namespace = Namespace,
                uid = UID
            } = ActorId,
            Tags = #{
                <<"actor.group">> => Group,
                <<"actor.resource">> => Res,
                <<"actor.name">> => Name,
                <<"actor.namespace">> => Namespace,
                <<"actor.uid">> => UID,
                <<"actor.pid">> => list_to_binary(pid_to_list(self()))
            },
            nkactor_srv_lib:op_span_tags(Tags, State2),
            case check_expire_time(State2) of
                {false, State3} ->
                    nkactor_srv_lib:op_span_log(<<"registering with namespace">>, State3),
                    case do_register(1, State3) of
                        {ok, State4} ->
                            nkactor_srv_lib:op_span_log(<<"registered with namespace">>, State4),
                            case nkactor_srv_lib:handle(actor_srv_init, [Op], State4) of
                                {ok, State5} ->
                                    State6 = nkactor_srv_lib:set_times(State5),
                                    case do_post_init(Op, State6) of
                                        {ok, State7} ->
                                            {ok, State7};
                                        {error, Error} ->
                                            do_init_stop(Error, Caller, Ref, State6)
                                    end;
                                {error, Error} ->
                                    do_init_stop(Error, Caller, Ref, State4);
                                {delete, Error} ->
                                    _ = nkactor_srv_lib:delete(State4),
                                    do_init_stop(Error, Caller, Ref, State4)
                            end;
                        {error, Error} ->
                            do_init_stop(Error, Caller, Ref, State3)
                    end;
                {true, State3} ->
                    nkactor_srv_lib:op_span_log(<<"actor is expired on load">>, State3),
                    ?ACTOR_LOG(info, "actor is expired on load", [], State3),
                    _ = do_stop(actor_expired, State3),
                    do_init_stop(actor_expired, Caller, Ref, State3)
            end
    end.


%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {noreply, state()} | {reply, term(), state()} |
    {stop, Reason :: term(), state()} | {stop, Reason :: term(), Reply :: term(), state()}.

handle_call({nkactor_sync_op, Op}, From, State) ->
    case nkactor_srv_lib:handle(actor_srv_sync_op, [Op, From], State) of
        {reply, Reply, #actor_st{} = State2} ->
            reply(Reply, do_refresh_ttl(State2));
        {reply_and_save, Reply, #actor_st{} = State2} ->
            {_, State3} = nkactor_srv_lib:save(user_op, nkactor_srv_lib:set_updated(State2)),
            reply(Reply, do_refresh_ttl(State3));
        {noreply, #actor_st{} = State2} ->
            noreply(do_refresh_ttl(State2));
        {noreply_and_save, #actor_st{} = State2} ->
            {_, State3} = nkactor_srv_lib:save(user_op, nkactor_srv_lib:set_updated(State2)),
            noreply(do_refresh_ttl(State3));
        {stop, Reason, Reply, #actor_st{} = State2} ->
            gen_server:reply(From, Reply),
            do_stop(Reason, State2);
        {stop, Reason, #actor_st{} = State2} ->
            do_stop(Reason, State2);
        continue ->
            do_sync_op(Op, From, State);
        {continue, [Op2, _From2, #actor_st{} = State2]} ->
            do_sync_op(Op2, From, State2);
        Other ->
            ?ACTOR_LOG(error, "invalid response for sync op ~p: ~p", [Op, Other], State),
            error(invalid_sync_response)
    end;

handle_call(Msg, From, State) ->
    safe_handle(actor_srv_handle_call, [Msg, From], State).


%% @private
-spec handle_cast(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.

handle_cast({nkactor_async_op, Op}, State) ->
    case nkactor_srv_lib:handle(actor_srv_async_op, [Op], State) of
        {noreply, #actor_st{} = State2} ->
            noreply(do_refresh_ttl(State2));
        {noreply_and_save, #actor_st{} = State2} ->
            {_, State3} = nkactor_srv_lib:save(user_op, nkactor_srv_lib:set_updated(State2)),
            noreply(do_refresh_ttl(State3));
        {stop, Reason, #actor_st{} = State2} ->
            do_stop(Reason, State2);
        continue ->
            do_async_op(Op, State);
        {continue, [Op2, #actor_st{} = State2]} ->
            do_async_op(Op2, State2);
        Other ->
            ?ACTOR_LOG(error, "invalid response for async op ~p: ~p", [Op, Other], State),
            error(invalid_async_response)
    end;

handle_cast(nkactor_hibernate, State) ->
    {_, State2} = nkactor_srv_lib:save(hibernate, State),
    {noreply, State2, hibernate};

handle_cast(Msg, State) ->
    safe_handle(actor_srv_handle_cast, [Msg], State).


%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.

handle_info({nkactor_updated, _SrvId}, State) ->
    set_debug(State),
    noreply(State);

handle_info(nkactor_ttl_timeout, State) ->
    do_ttl_timeout(State);

handle_info(nkactor_timer_save, State) ->
    do_async_op({save, timer}, State);

handle_info(nkactor_check_activate_time, State) ->
    {noreply, check_activate_time(State)};

handle_info(nkactor_check_expire_time, State) ->
    case check_expire_time(State) of
        {true, State2} ->
            do_stop(actor_expired, State2);
        {false, State2} ->
            {noreply, State2}
    end;

handle_info(nkactor_heartbeat, #actor_st{config = Config} = State) ->
    HeartbeatTime = maps:get(heartbeat_time, Config),
    erlang:send_after(HeartbeatTime, self(), nkactor_heartbeat),
    do_heartbeat(State);

handle_info({'DOWN', _Ref, process, Pid, normal}, #actor_st{namespace_pid = Pid} = State) ->
    ?ACTOR_LOG(notice, "namespace is stopping, we also stop", [], State),
    do_stop(namespace_is_down, State);

handle_info({'DOWN', _Ref, process, Pid, _Reason}, #actor_st{namespace_pid = Pid} = State) ->
    ?ACTOR_LOG(notice, "namespace is down", [], State),
    case do_register(5, State#actor_st{namespace_pid = undefined}) of
        {ok, State2} ->
            ?ACTOR_LOG(notice, "re-registered with namespace", [], State),
            noreply(State2);
        {error, _Error} ->
            do_stop(namespace_is_down, State)
    end;

handle_info({'DOWN', Ref, process, _Pid, Reason} = Info, State) ->
    case link_down(Ref, State) of
        {ok, Link, #link_info{data = Data} = LinkInfo, State2} ->
            ?ACTOR_LOG(warning, "link ~p down (~p) (~p)", [Link, LinkInfo, Reason], State),
            ?ACTOR_DEBUG("link ~p down (~p) (~p)", [Link, LinkInfo, Reason], State),
            {ok, State3} = nkactor_srv_lib:handle(actor_srv_link_down, [Link, Data], State2),
            noreply(State3);
        not_found ->
            safe_handle(actor_srv_handle_info, [Info], State)
    end;

handle_info(Msg, State) ->
    safe_handle(actor_srv_handle_info, [Msg], State).


%% @private
-spec code_change(term(), state(), term()) ->
    {ok, state()}.

code_change(OldVsn, #actor_st{srv = SrvId} = State, Extra) ->
    ?CALL_SRV(SrvId, actor_code_change, [OldVsn, State, Extra]).


%% @private
-spec terminate(term(), state()) ->
    ok.

%%terminate(_Reason, #actor{moved_to=Pid}) when is_pid(Pid) ->
%%    ok;

terminate(Reason, #actor_st{op_span_ids=SpanIds}=State) ->
    State2 = do_stop2({terminate, Reason}, State),
    {ok, _State3} = nkactor_srv_lib:handle(actor_srv_terminate, [Reason], State2),
    lists:foreach(
        fun(SpanId) ->
            nkserver_ot:log(SpanId, <<"actor terminated: ~p">>, [Reason]),
            nkserver_ot:finish(SpanId)
        end,
        SpanIds),
    ok.


%% ===================================================================
%% Operations
%% ===================================================================

%% @private
do_sync_op(get_actor_id, _From, #actor_st{actor_id=ActorId}=State) ->
    reply({ok, ActorId}, do_refresh_ttl(State));

do_sync_op(get_srv_uid, _From, #actor_st{srv=SrvId, actor_id=ActorId}=State) ->
    #actor_id{uid=UID} = ActorId,
    reply({ok, SrvId, UID}, do_refresh_ttl(State));

do_sync_op(get_actor, _From, State) ->
    {UserActor, State2} = do_get_user_actor(State),
    reply({ok, UserActor}, do_refresh_ttl(State2));

do_sync_op(consume_actor, From, State) ->
    {UserActor, State2} = do_get_user_actor(State),
    gen_server:reply(From, {ok, UserActor}),
    {_, State3} = nkactor_srv_lib:delete(State2),
    do_stop(actor_consumed, State3);

do_sync_op(get_state, _From, State) ->
    reply({ok, State}, State);

do_sync_op(get_links, _From, State) ->
    Data = nkactor_srv_lib:get_links(State),
    reply({ok, Data}, State);

do_sync_op(save, _From, State) ->
    {Reply, State2} = nkactor_srv_lib:save(user_order, State),
    reply(Reply, do_refresh_ttl(State2));

do_sync_op(force_save, _From, State) ->
    {Reply, State2} = nkactor_srv_lib:save(user_order, nkactor_srv_lib:set_updated(State)),
    reply(Reply, do_refresh_ttl(State2));

do_sync_op(get_timers, _From, State) ->
    #actor_st{
        actor = #{metadata:=Meta},
        ttl_timer = TTL,
        activate_timer = Activate,
        expire_timer = Expire,
        save_timer = Save,
        unload_policy = Policy
    } = State,
    Data = #{
        policy => Policy,
        ttl_timer => case is_reference(TTL) of true -> erlang:read_timer(TTL); _ -> undefined end,
        activate_timer => case is_reference(Activate) of true -> erlang:read_timer(Activate); _ -> undefined end,
        expire_timer => case is_reference(Expire) of true -> erlang:read_timer(Expire); _ -> undefined end,
        save_timer => case is_reference(Save) of true -> erlang:read_timer(Save); _ -> undefined end,
        expire_time => maps:get(expire_time, Meta, <<>>),
        activate_time => maps:get(activate_time, Meta, <<>>)
    },
    reply({ok, Data}, State);

do_sync_op(delete, From, #actor_st{is_enabled=IsEnabled, config=Config}=State) ->
    case {IsEnabled, Config} of
        {false, #{dont_delete_on_disabled:=true}} ->
            reply({error, actor_is_disabled}, State);
        _ ->
            {Reply, State2} = nkactor_srv_lib:delete(State),
            gen_server:reply(From, Reply),
            do_stop(actor_deleted, State2)
    end;

do_sync_op({enable, Enable}, _From, State) when is_boolean(Enable)->
    #actor_st{actor = #{metadata := Meta} = Actor} = State,
    case maps:get(is_enabled, Meta, true) of
        Enable ->
            reply(ok, do_refresh_ttl(State));
        _ ->
            Meta2 = Meta#{is_enabled => Enable},
            UpdActor = Actor#{metadata := Meta2},
            case nkactor_srv_lib:update(UpdActor, #{}, State) of
                {ok, State2} ->
                    reply(ok, do_refresh_ttl(State2));
                {error, Error, State2} ->
                    reply({error, Error}, State2)
            end
    end;

do_sync_op(is_enabled, _From, #actor_st{is_enabled=IsEnabled}=State) ->
    reply({ok, IsEnabled}, State);

do_sync_op({link, Link, Opts}, _From, State) ->
    State2 = nkactor_srv_lib:add_link(Link, Opts, State),
    reply(ok, do_refresh_ttl(State2));

do_sync_op({update, Actor, Opts}, _From, State) ->
    #actor_st{is_enabled=IsEnabled, config=Config} = State,
    case {IsEnabled, Config} of
        {false, #{dont_update_on_disabled:=true}} ->
            reply({error, actor_is_disabled}, State);
        _ ->
            case nkactor_srv_lib:update(Actor, Opts, State) of
                {ok, State2} ->
                    ReplyActor = case Opts of
                        #{get_actor:=true} ->
                            State2#actor_st.actor;
                        _ ->
                            #{}
                    end,
                    reply({ok, ReplyActor}, do_refresh_ttl(State2));
                {error, Error, State2} ->
                    reply({error, Error}, State2)
            end
    end;

do_sync_op(get_alarms, _From, #actor_st{actor=Actor}=State) ->
    Alarms = case Actor of
        #{metadata := #{in_alarm:=true, alarms:=AlarmList}} ->
            AlarmList;
        _ ->
            []
    end,
    reply({ok, Alarms}, State);

do_sync_op({set_alarm, Alarm}, _From, State) ->
    case nkactor_srv_lib:add_actor_alarm(Alarm, State) of
        {ok, State2} ->
            reply(ok, do_refresh_ttl(State2));
        {error, Error} ->
            reply({error, Error}, State)
    end;

do_sync_op({apply, Mod, Fun, Args}, From, State) ->
    apply(Mod, Fun, Args++[From, do_refresh_ttl(State)]);

do_sync_op(Op, _From, State) ->
    ?ACTOR_LOG(notice, "unknown sync op: ~p", [Op], State),
    reply({error, unknown_op}, State).


%% @private
do_async_op({unlink, Link}, State) ->
    case nkactor_srv_lib:remove_link(Link, State) of
        {true, State2} ->
            {ok, State2};
        false ->
            {ok, State}
    end;

do_async_op({send_info, Info, Meta}, State) ->
    noreply(nkactor_srv_lib:event({info, nklib_util:to_binary(Info), Meta}, do_refresh_ttl(State)));

do_async_op({send_event, Event}, State) ->
    noreply(nkactor_srv_lib:event(Event, do_refresh_ttl(State)));

do_async_op({set_activate_time, Time}, State) ->
    noreply(nkactor_srv_lib:set_activate_time(Time, State));

do_async_op({set_dirty, true}, State) ->
    noreply(do_refresh_ttl(nkactor_srv_lib:set_updated(State)));

do_async_op({set_dirty, false}, State) ->
    noreply(do_refresh_ttl(State#actor_st{is_dirty = false}));

do_async_op(save, State) ->
    do_async_op({save, user_order}, State);

do_async_op({save, Reason}, State) ->
    {_Reply, State2} = nkactor_srv_lib:save(Reason, State),
    noreply(do_refresh_ttl(State2));

do_async_op(delete, #actor_st{is_enabled=IsEnabled, config=Config}=State) ->
    case {IsEnabled, Config} of
        {false, #{dont_delete_on_disabled:=true}} ->
            noreply(State);
        _ ->
            {_Reply, State2} = nkactor_srv_lib:delete(State),
            do_stop(actor_deleted, State2)
    end;
do_async_op({stop, Reason}, State) ->
    ?ACTOR_DEBUG("received stop: ~p", [Reason], State),
    do_stop(Reason, State);

do_async_op({raw_stop, Reason}, State) ->
    % We don't send the deleted event here, since we may not be active at all
    case Reason of
        backend_deleted ->
            ok;
        _ ->
            ?ACTOR_LOG(info, "received raw_stop: ~p", [Reason], State)
    end,
    {_, State2} = nkactor_srv_lib:handle(actor_srv_stop, [Reason], State),
    State3 = nkactor_srv_lib:event({stopped, Reason}, State2),
    {stop, normal, State3#actor_st{stop_reason=raw_stop}};

do_async_op({set_alarm, Alarm}, State) ->
    case nkactor_srv_lib:add_actor_alarm(Alarm, State) of
        {ok, State2} ->
            noreply(do_refresh_ttl(State2));
        {error, _} ->
            ?ACTOR_LOG(error, "invalid alarm: ~p", [Alarm], State),
            noreply(State)
    end;

do_async_op(clear_all_alarms, State) ->
    State2 = nkactor_srv_lib:clear_all_alarms(State),
    noreply(do_refresh_ttl(State2));

do_async_op(Op, State) ->
    ?ACTOR_LOG(notice, "unknown async op: ~p", [Op], State),
    noreply(State).


%% ===================================================================
%% Internals
%% ===================================================================

%% @private
do_pre_init(ActorId, Actor, Config, SrvId) ->
    #actor_id{uid=UID}=ActorId,
    #{metadata:=Meta} = Actor,
    ActorId2 = ActorId#actor_id{pid=self()},
    true = is_binary(UID) andalso UID /= <<>>,
    #actor_st{
        srv = SrvId,
        module = maps:get(module, Config),
        config = Config,
        actor_id = ActorId2,
        actor = Actor,
        run_state = undefined,
        links = nklib_links:new(),
        is_dirty = false,
        saved_metadata = Meta,
        is_enabled = maps:get(is_enabled, Meta, true),
        activate_timer = undefined,
        expire_timer = undefined,
        save_timer = undefined,
        unload_policy = permanent,           % Updated later
        op_span_ids = []
    }.


%% @private
do_post_init(Op, State) ->
    #actor_st{config=Config} = State,
    ?ACTOR_DEBUG("started (~p)", [self()], State),
    State2 = case Op of
        create ->
            nkactor_srv_lib:op_span_log(<<"starting creation save">>, State),
            % Do not update generation
            State#actor_st{is_dirty=true};
        _ ->
            State
    end,
    case nkactor_srv_lib:save(Op, State2) of
        {ok, State3} ->
            State4 = case Op of
                create ->
                    nkactor_srv_lib:op_span_log(<<"actor saved">>, State),
                    nkactor_srv_lib:event(created, State3);
                _ ->
                    nkactor_srv_lib:op_span_log(<<"actor activated">>, State),
                    nkactor_srv_lib:event(activated, State3)
            end,
            State5 = do_check_alarms(State4),
            case Config of
                #{heartbeat_time:=HeartbeatTime} ->
                    erlang:send_after(HeartbeatTime, self(), nkactor_heartbeat);
                _ ->
                    ok
            end,
            nklib_counters:async([?MODULE]),
            nkactor_srv_lib:op_span_log(<<"actor init completed">>, State5),
            State6 = nkactor_srv_lib:op_span_finish(State5),
            {ok, do_refresh_ttl(State6)};
        {{error, Error}, State3} ->
            nkactor_srv_lib:op_span_log("actor save error: ~p", [Error], State3),
            {error, Error}
    end.


%% @private
do_init_stop(Error, Caller, Ref, State) ->
    nkactor_srv_lib:op_span_log("actor init error: ~p", [Error], State),
    nkactor_srv_lib:op_span_error(Error, State),
    _State2 = nkactor_srv_lib:op_span_finish(State),
    Caller ! {do_start_ignore, Ref, {error, Error}},
    ignore.


%% @private
set_debug(#actor_st{srv=SrvId, actor_id=ActorId}=State) ->
    #actor_id{group=Group, resource=Resource} = ActorId,
    Debug = case catch nkserver:get_cached_config(SrvId, nkactor, debug_actors) of
        List when is_list(List) ->
            lists:member(<<"all">>, List) orelse
                lists:member(Group, List) orelse
                lists:member(<<Group/binary, $/, Resource/binary>>, List);
        _ ->
            false
    end,
    put(nkactor_debug, Debug),
    ?ACTOR_DEBUG("debug activated", [], State).


%% @private
do_register(Tries, #actor_st{actor_id=ActorId, srv=SrvId} = State) ->
    case nkactor_namespace:register_actor(ActorId) of
        {ok, SrvId, Pid} ->
            State2 = State#actor_st{namespace_pid=Pid},
            monitor(process, Pid),
            set_debug(State2),
            ?ACTOR_DEBUG("registered with namespace (pid:~p)", [Pid]),
            {ok, State2};
        {error, actor_already_registered} ->
            {error, actor_already_registered};
        {error, Error} ->
            case Tries > 1 of
                true ->
                    ?ACTOR_LOG(notice, "registered with namespace failed (~p) (~p tries left)",
                        [Error, Tries], State),
                    timer:sleep(1000),
                    do_register(Tries - 1, State);
                false ->
                    ?ACTOR_LOG(notice, "registration with namespace failed: ~p", [Error], State),
                    {error, Error}
            end
    end.


%% @private
do_check_alarms(#actor_st{actor=#{metadata:=Meta}}=State) ->
    case Meta of
        #{in_alarm:=true} ->
            {ok, State2} = nkactor_srv_lib:handle(actor_srv_alarms, [], State),
            State2;
        _ ->
            State
    end.


%% @private Reset TTL and check dirty
do_refresh_ttl(#actor_st{unload_policy={ttl, Time}, ttl_timer=Timer}=State) when is_integer(Time) ->
    nklib_util:cancel_timer(Timer),
    Ref = erlang:send_after(Time, self(), nkactor_ttl_timeout),
    do_check_save(State#actor_st{ttl_timer=Ref});

do_refresh_ttl(State) ->
    do_check_save(State).


%% @private
do_stop(Reason, State) ->
    {stop, normal, do_stop2(Reason, State)}.


%% @private
do_stop2(Reason, #actor_st{stop_reason=false}=State) ->
    State2 = State#actor_st{stop_reason=Reason},
    State3 = nkactor_srv_lib:event({stopped, Reason}, State2),
    case nkactor_srv_lib:handle(actor_srv_stop, [Reason], State3) of
        {ok, State4} ->
            {_, State5} = nkactor_srv_lib:save(unloaded, State4),
            State5;
        {delete, State4} ->
            {_, State5} = nkactor_srv_lib:delete(State4),
            State5
    end;

do_stop2(_Reason, State) ->
    State.


%% @private
do_heartbeat(State) ->
    case nkactor_srv_lib:handle(actor_srv_heartbeat, [], State) of
        {ok, State2} ->
            noreply(State2);
        {error, Error} ->
            do_stop(Error, State)
    end.


%% @private
do_check_save(#actor_st{is_dirty=true, save_timer=undefined, config=Config}=State) ->
    case maps:get(save_time, Config, undefined) of
        undefined ->
            State;
        SaveTime ->
            Timer = erlang:send_after(SaveTime, self(), nkactor_timer_save),
            State#actor_st{save_timer=Timer}
    end;

do_check_save(State) ->
    State.


%% @private
do_ttl_timeout(#actor_st{unload_policy={ttl, _}, links=Links, activate_timer=undefined}=State) ->
    Avoid = nklib_links:fold_values(
        fun
            (_Link, #link_info{avoid_unload=true}, _Acc) -> true;
            (_Link, _LinkOpts, Acc) -> Acc
        end,
        false,
        Links),
    case Avoid of
        true ->
            noreply(do_refresh_ttl(State));
        false ->
            do_stop(ttl_timeout, State)
    end;

do_ttl_timeout(State) ->
    noreply(do_refresh_ttl(State)).


%% @private
check_activate_time(#actor_st{actor=Actor, activate_timer=Timer1}=State) ->
    nklib_util:cancel_timer(Timer1),
    case Actor of
        #{metadata:=#{activate_time:=ActiveTime}=Meta} ->
            Now = nklib_date:epoch(msecs),
            {ok, ActiveTime2} = nklib_date:to_epoch(ActiveTime, msecs),
            case ActiveTime2 - Now of
                Step when Step=<0 ->
                    ?ACTOR_LOG(info, "activation time!", [], State),
                    Meta2 = maps:remove(activate_time, Meta),
                    State2 = State#actor_st{
                        activate_timer = undefined,
                        actor = Actor#{metadata:=Meta2}
                    },
                    {ok, State3} = nkactor_srv_lib:handle(actor_srv_activate_timer, [ActiveTime], State2),
                    State3;
                Step ->
                    Step2 = min(Step, ?MAX_STATUS_TIME),
                    ?ACTOR_LOG(info, "no yet activation time, ~pmsecs left (next call in ~pmsecs)", [Step, Step2], State),
                    Timer2 = erlang:send_after(Step2, self(), nkactor_check_activate_time),
                    State#actor_st{activate_timer=Timer2}
            end;
        _ ->
            State
    end.


%% @private
check_expire_time(#actor_st{actor=Actor, expire_timer=Timer1}=State) ->
    nklib_util:cancel_timer(Timer1),
    case Actor of
        #{metadata:=#{expire_time:=ExpireTime}=Meta} ->
            Now = nklib_date:epoch(msecs),
            {ok, ExpireTime2} = nklib_date:to_epoch(ExpireTime, msecs),
            case ExpireTime2 - Now of
                Step when Step=<0 ->
                    ?ACTOR_LOG(info, "expiration time!", [], State),
                    Meta2 = maps:remove(expire_time, Meta),
                    State2 = State#actor_st{
                        expire_timer = undefined,
                        actor = Actor#{metadata:=Meta2}
                    },
                    case nkactor_srv_lib:handle(actor_srv_expired, [ExpireTime], State2) of
                        {ok, State3} ->
                            {true, State3};
                        {delete, State3} ->
                            {_, State4} = nkactor_srv_lib:delete(State3),
                            {true, State4}
                    end;
                Step ->
                    Step2 = min(Step, ?MAX_STATUS_TIME),
                    ?ACTOR_LOG(info, "no yet expiration time, ~pmsecs left (next call in ~pmsecs)", [Step, Step2], State),
                    Timer2 = erlang:send_after(Step2, self(), nkactor_check_expire_time),
                    {false, State#actor_st{expire_timer=Timer2}}
            end;
        _ ->
            {false, State}
    end.



%% ===================================================================
%% Util
%% ===================================================================


% @private
reply(Reply, #actor_st{}=State) ->
    {reply, Reply, State}.


%% @private
noreply(#actor_st{}=State) ->
    {noreply, State}.


%% @private
safe_handle(Fun, Args, State) ->
    Reply = nkactor_srv_lib:handle(Fun, Args, State),
    case Reply of
        {reply, _, #actor_st{}} ->
            Reply;
        {reply, _, #actor_st{}, _} ->
            Reply;
        {noreply, #actor_st{}} ->
            Reply;
        {noreply, #actor_st{}, _} ->
            Reply;
        {stop, _, _, #actor_st{}} ->
            Reply;
        {stop, _, #actor_st{}} ->
            Reply;
        Other ->
            ?ACTOR_LOG(error, "invalid response for ~p(~p): ~p", [Fun, Args, Other], State),
            error(invalid_handle_response)
    end.


%% @private
do_get_user_actor(#actor_st{actor = Actor}=State) ->
    {ok, UserActor, State2} = nkactor_srv_lib:handle(actor_srv_get, [Actor], State),
    {UserActor, State2}.


%% @private
link_down(Mon, #actor_st{links=Links}=State) ->
    case nklib_links:down(Mon, Links) of
        {ok, Link, #link_info{data=Data}=LinkInfo, Links2} ->
            case LinkInfo of
                #link_info{gen_events=true, data=Data} ->
                    State2 = nkactor_srv_lib:event({link_down, Data}, State),
                    {ok, Link, LinkInfo, State2#actor_st{links=Links2}};
                _ ->
                    {ok, Link, LinkInfo, State#actor_st{links=Links2}}
            end;
        not_found ->
            not_found
    end.

%%%% @private
%%op_span_update_srv_id(SrvId, #actor_st{op_span_ids=[SpanId|_]}) ->
%%    nkserver_ot:update_srv_id(SpanId, SrvId);
%%
%%op_span_update_srv_id(_SrvId, #actor_st{op_span_ids=[]}) ->
%%    ok.


%%%% @private
%%links_iter(Fun, Acc, #actor_st{links=Links}) ->
%%    nklib_links:fold_values(Fun, Acc, Links).

%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).
