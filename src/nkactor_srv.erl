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
%%      if object has permanent => true in config(), it is not unloaded
%% - Expires mode
%%      if object has expires property, the object expires after that time no matter what
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
-export([do_event/2, do_event_link/2, do_update/3, do_delete/1, do_set_next_status_time/2,
    do_unset_next_status_time/1, do_get_links/1, do_add_link/3, do_remove_link/2,
    do_save/2]).
-export_type([event/0, save_reason/0]).

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver_ot/include/nkserver_ot.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(DEFAULT_TTL, 10000).
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
    {alarm_fired, nkactor:alarm_class(), nkactor:alarm_body()} |
    alarms_all_cleared |
    {stopped, nkserver:msg()}.


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
    {set_alarm, nkactor:alarm_class(), nkactor:alarm_body()} |
    {apply, Mod :: module(), Fun :: atom(), Args :: list()} |
    term().

-type async_op() ::
    {unlink, nklib:link()} |
    {send_info, atom()|binary(), map()} |
    {send_event, event()} |
    {set_next_status_time, binary()|integer()} |
    {set_alarm, nkactor:alarm_class(), nkactor:alarm_body()} |
    clear_all_alarms |
    save |
    delete |
    {stop, Reason :: nkserver:msg()} |
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



-record(link_info, {
    get_events :: boolean(),
    gen_events :: boolean(),
    avoid_unload :: boolean(),
    data :: term()
}).


%% ===================================================================
%% Public
%% ===================================================================

%% @private
%% Call nkactor:create/2 instead calling this directly!
-spec create(nkactor:actor(), start_opts()) ->
    {ok, #actor_id{}} | {error, term()}.

create(Actor, Config) ->
    do_start(create, Actor, Config).


%% @private
%% Call nkactor:activate/1 instead calling this directly!
-spec start(nkactor:actor(), start_opts()) ->
    {ok, #actor_id{}} | {error, term()}.

start(Actor, Config) ->
    do_start(start, Actor, Config).


%% @private
do_start(Op, Actor, StartConfig) ->
    ActorId = nkactor_lib:actor_to_actor_id(Actor),
    case nkactor_util:get_actor_config(ActorId) of
        {ok, _SrvId, #{activable:=false}} ->
            {error, actor_is_not_activable};
        {ok, SrvId, BaseConfig} ->
            Ref = make_ref(),
            Config = maps:merge(BaseConfig, StartConfig),
            Opts = {Op, ActorId, Actor, Config, SrvId, self(), Ref},
            case gen_server:start(?MODULE, Opts, []) of
                {ok, Pid} ->
                    {ok, ActorId#actor_id{pid = Pid}};
                ignore ->
                    receive
                        {do_start_ignore, Ref, Result} ->
                            Result
                    after 5000 ->
                        error(do_start_ignore)
                    end;
                {error, Error} ->
                    {error, Error}
            end
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
sync_op(_Id, _Op, _Timeout, 0) ->
    {error, process_not_found};

sync_op(Id, Op, Timeout, Tries) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case ActorId of
        #actor_id{pid = Pid} when is_pid(Pid) ->
            case sync_op(Pid, Op, Timeout) of
                {error, process_not_found} ->
                    ActorId2 = ActorId#actor_id{pid = undefined},
                    timer:sleep(250),
                    lager:warning("NkACTOR SynOP failed (~p), retrying...", [ActorId2]),
                    sync_op(ActorId2, Op, Timeout, Tries - 1);
                Other ->
                    Other
            end;
        _ ->
            case nkactor:activate(ActorId) of
                {ok, #actor_id{pid = Pid} = ActorId2} when is_pid(Pid) ->
                    sync_op(ActorId2, Op, Timeout, Tries);
                {error, Error} ->
                    {error, Error}
            end
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


%% ===================================================================
%% In-process API
%% ===================================================================

%% @private
do_event(Event, State) ->
    ?ACTOR_DEBUG("sending 'event': ~p", [Event], State),
    State2 = do_event_link(Event, State),
    {ok, State3} = handle(actor_srv_event, [Event], State2),
    State3.


%% @private
do_event_link(Event, #actor_st{links = Links} = State) ->
    nklib_links:fold_values(
        fun
            (Link, #link_info{get_events = true, data = Data}, Acc) ->
                {ok, Acc2} = handle(actor_srv_link_event, [Link, Data, Event], Acc),
                Acc2;
            (_Link, _LinkOpts, Acc) ->
                Acc
        end,
        State,
        Links).


%% @private
do_set_next_status_time(NextTime, #actor_st{actor = Actor} = State) ->
    {ok, NextTime2} = nklib_date:to_3339(NextTime, msecs),
    #{metadata:=Meta} = Actor,
    Meta2 = Meta#{next_status_time => NextTime2},
    Actor2 = Actor#{metadata:=Meta2},
    self() ! nkactor_check_next_status_time,
    State#actor_st{actor = Actor2, is_dirty = true}.


%% @private
do_unset_next_status_time(#actor_st{actor = Actor, status_timer = Timer} = State) ->
    nklib_util:cancel_timer(Timer),
    #{metadata:=Meta} = Actor,
    case maps:is_key(next_status_time, Meta) of
        true ->
            Meta2 = maps:remove(next_status_time, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            State#actor_st{actor = Actor2, is_dirty = true};
        false ->
            State
    end.


%% @private
do_get_links(#actor_st{links = Links}) ->
    nklib_links:fold_values(
        fun(Link, #link_info{data = Data}, Acc) -> [{Link, Data} | Acc] end,
        [],
        Links).


%% @private
do_add_link(Link, Opts, #actor_st{links = Links} = State) ->
    LinkInfo = #link_info{
        get_events = maps:get(get_events, Opts, false),
        gen_events = maps:get(gen_events, Opts, true),
        avoid_unload = maps:get(avoid_unload, Opts, false),
        data = maps:get(data, Opts, undefined)
    },
    State2 = case LinkInfo#link_info.gen_events of
        true ->
            do_event({link_added, Link}, State);
        _ ->
            State
    end,
    State2#actor_st{links = nklib_links:add(Link, LinkInfo, Links)}.


%% @private
do_remove_link(Link, #actor_st{links = Links} = State) ->
    case nklib_links:get_value(Link, Links) of
        {ok, #link_info{gen_events = true}} ->
            State2 = do_event({link_removed, Link}, State),
            State3 = State2#actor_st{links = nklib_links:remove(Link, Links)},
            {true, State3};
        not_found ->
            false
    end.


%% @private
%% It will create an 'operation span' if ot_span_id option is used
do_update(UpdActor, Opts, #actor_st{srv = SrvId, actor_id = ActorId, actor = Actor} = State) ->
    UpdActorId = nkactor_lib:actor_to_actor_id(UpdActor),
    #actor_id{uid = UID, namespace = Namespace, group = Class, resource = Res, name = Name} = ActorId,
    try
        case UpdActorId#actor_id.namespace of
            Namespace -> ok;
            _ -> throw({updated_invalid_field, namespace})
        end,
        case UpdActorId#actor_id.uid of
            undefined -> ok;
            UID -> ok;
            _ -> throw({updated_invalid_field, uid})
        end,
        case UpdActorId#actor_id.group of
            Class -> ok;
            _ -> throw({updated_invalid_field, group})
        end,
        case UpdActorId#actor_id.resource of
            Res -> ok;
            _ -> throw({updated_invalid_field, resource})
        end,
        case UpdActorId#actor_id.name of
            Name -> ok;
            _ -> throw({updated_invalid_field, name})
        end,
        DataFieldsList = maps:get(data_fields, Opts, all),
        Data = maps:get(data, Actor, #{}),
        Meta = maps:get(metadata, Actor),
        DataFields = case DataFieldsList of
            all ->
                Data;
            _ ->
                maps:with(DataFieldsList, Data)
        end,
        UpdData = maps:get(data, UpdActor, #{}),
        UpdDataFields = case DataFieldsList of
            all ->
                UpdData;
            _ ->
                maps:with(DataFieldsList, UpdData)
        end,
        IsDataUpdated = UpdDataFields /= DataFields,
        UpdMeta = maps:get(metadata, UpdActor),
        CT = maps:get(creation_time, Meta),
        case maps:get(creation_time, UpdMeta, CT) of
            CT -> ok;
            _ -> throw({updated_invalid_field, metadata})
        end,
        SubType = maps:get(subtype, Meta, <<>>),
        case maps:get(subtype, UpdMeta, <<>>) of
            SubType -> ok;
            _ -> throw({updated_invalid_field, subtype})
        end,
        Links = maps:get(links, Meta, #{}),
        UpdLinks = maps:get(links, UpdMeta, Links),
        UpdMeta2 = case UpdLinks == Links of
            true ->
                UpdMeta;
            false ->
                case nkactor_lib:do_check_links(SrvId, UpdMeta) of
                    {ok, UpdMetaLinks} ->
                        UpdMetaLinks;
                    {error, Error} ->
                        throw(Error)
                end
        end,
        Enabled = maps:get(is_enabled, Meta, true),
        UpdEnabled = maps:get(is_enabled, UpdMeta2, true),
        UpdMeta3 = case maps:get(is_enabled, UpdMeta2, true) of
            true ->
                maps:remove(is_enabled, UpdMeta2);
            false ->
                UpdMeta2
        end,
        NewMeta = maps:merge(Meta, UpdMeta3),
        IsMetaUpdated = (Meta /= NewMeta) orelse (Enabled /= UpdEnabled),
        case IsDataUpdated orelse IsMetaUpdated of
            true ->
                % At this point, we create main span and operation span
                State2 = op_span_check_create(update, Opts, State),
                %lager:error("NKLOG UPDATE Data:~p, Meta:~p", [IsDataUpdated, IsMetaUpdated]),
                NewMeta2 = case UpdEnabled of
                    true ->
                        maps:remove(is_enabled, NewMeta);
                    false ->
                        NewMeta#{is_enabled=>false}
                end,
                Data2 = maps:merge(Data, UpdDataFields),
                NewActor = Actor#{data=>Data2, metadata:=NewMeta2},
                case nkactor_lib:update_check_fields(NewActor, State2) of
                    ok ->
                        op_span_log(<<"calling actor_srv_update">>, State2),
                        case handle(actor_srv_update, [NewActor], State2) of
                            {ok, NewActor2, State3} ->
                                State4 = State3#actor_st{actor = NewActor2, is_dirty = true},
                                State5 = do_enabled(UpdEnabled, State4),
                                State6 = set_unload_policy(State5),
                                op_span_log(<<"calling do_save">>, State6),
                                case do_save(update, #{}, State6) of
                                    {ok, State7} ->
                                        nkserver_ot:log(<<"actor updated">>, State7),
                                        op_span_log(<<"actor updated">>, State7),
                                        State8 = op_span_finish(State7),
                                        {ok, do_event({updated, UpdActor}, State8)};
                                    {{error, SaveError}, State7} ->
                                        op_span_log(<<"save error: ~p">>, [SaveError], State7),
                                        op_span_error(SaveError, State7),
                                        State8 = op_span_finish(State7),
                                        {error, SaveError, State8}
                                end;
                            {error, UpdError, State3} ->
                                op_span_log(<<"update error: ~p">>, [UpdError], State3),
                                op_span_error(UpdError, State3),
                                State4 = op_span_finish(State3),
                                {error, UpdError, State4}
                        end;
                    {error, StaticFieldError} ->
                        op_span_log(<<"update error: ~p">>, [StaticFieldError], State2),
                        op_span_error(StaticFieldError, State2),
                        State4 = op_span_finish(State2),
                        {error, StaticFieldError, State4}
                end;
            false ->
                % lager:error("NKLOG NO UPDATE"),
                {ok, State}
        end
    catch
        throw:Throw ->
            {error, Throw, State}
    end.


%% @private
do_delete(#actor_st{is_dirty = deleted} = State) ->
    {ok, State};

do_delete(#actor_st{srv = SrvId, actor_id = ActorId, actor = Actor} = State) ->
    #actor_id{uid = UID} = ActorId,
    case handle(actor_srv_delete, [], State) of
        {ok, State2} ->
            case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, [UID], #{}]) of
                {ok, _ActorIds, DbMeta} ->
                    ?ACTOR_DEBUG("object deleted: ~p", [DbMeta], State),
                    {ok, do_event(deleted, State2#actor_st{is_dirty = deleted})};
                {error, Error} ->
                    ?ACTOR_LOG(warning, "object could not be deleted: ~p", [Error], State),
                    {{error, Error}, State2#actor_st{actor = Actor}}
            end;
        {error, Error, State2} ->
            {{error, Error}, State2}
    end.


%% @private
do_save(Reason, State) ->
    do_save(Reason, #{}, State).


%% @private
do_save(Reason, SaveOpts, #actor_st{is_dirty = true} = State) ->
    #actor_st{
        srv = SrvId,
        actor = Actor,
        saved_metadata = SavedMeta,
        save_timer = Timer,
        config = Config
    } = State,
    nklib_util:cancel_timer(Timer),
    Actor2 = nkactor_lib:update(Actor, nklib_date:now_3339(usecs)),
    State2 = State#actor_st{actor = Actor2, save_timer = undefined},
    State3 = op_span_force_create(save, State2),
    #actor_st{op_span_ids=[SpanId|_]} = State3,
    {SaveFun, SaveOpts2} = case Reason of
        create ->
            CheckUnique = maps:get(create_check_unique, Config, true),
            {
                actor_db_create,
                SaveOpts#{check_unique=>CheckUnique, ot_span_id=>SpanId}
            };
        _ ->
            {
                actor_db_update,
                SaveOpts#{last_metadata=>SavedMeta, ot_span_id=>SpanId}}
    end,
    op_span_log(<<"calling actor_srv_save">>, State3),
    case handle(actor_srv_save, [Actor2], State2) of
        {ok, SaveActor, #actor_st{config = Config} = State3} ->
            case Config of
                #{async_save:=true} when Reason /= create ->
                    Self = self(),
                    Pid = spawn_link(
                        fun() ->
                            Span1 = nkserver_ot:span(SrvId, <<"ActorSrc::async_save">>, SpanId),
                            Span2 = nkserver_ot:log(Span1, <<"calling ~s">>, [SaveFun]),
                            case ?CALL_SRV(SrvId, SaveFun, [SrvId, SaveActor, SaveOpts2]) of
                                {ok, _DbMeta} ->
                                    Span3 = nkserver_ot:log(Span2, <<"actor saved">>),
                                    nkserver_ot:finish(Span3),
                                    async_op(Self, {send_event, saved});
                                {error, Error} ->
                                    Span3 = nkserver_ot:log(Span2, "save error: ~p", [Error]),
                                    Span4 = nkserver_ot:tag_error(Span3, Error),
                                    nkserver_ot:finish(Span4)
                            end
                        end),
                    op_span_log(<<"launched asynchronous save: ~p">>, [Pid], State3),
                    % We must guess that the save is successful
                    #{metadata:=NewMeta} = Actor2,
                    State4 = op_span_finish(State3),
                    {ok, State4#actor_st{saved_metadata = NewMeta, is_dirty = false}};
                _ ->
                    case ?CALL_SRV(SrvId, SaveFun, [SrvId, SaveActor, SaveOpts2]) of
                        {ok, DbMeta} ->
                            op_span_log(<<"actor saved">>, State3),
                            State4 = op_span_finish(State3),
                            ?ACTOR_DEBUG("save (~p) (~p)", [Reason, DbMeta], State4),
                            % The metadata of the updated actor is the new old metadata
                            % to check differences
                            #{metadata:=NewMeta} = Actor2,
                            State5 = State4#actor_st{saved_metadata = NewMeta, is_dirty = false},
                            {ok, do_event(saved, State5)};
                        {error, not_implemented} ->
                            op_span_log(<<"save not implemented">>, State3),
                            State4 = op_span_finish(State3),
                            {{error, not_implemented}, State4};
                        {error, Error} ->
                            op_span_log(<<"save error: ~p">>, Error, State3),
                            op_span_error(Error, State3),
                            State4 = op_span_finish(State3),
                            ?ACTOR_LOG(warning, "save error: ~p", [Error], State4),
                            {{error, Error}, State4}
                    end
            end;
        {ignore, State3} ->
            op_span_log(<<"save ignored">>, State3),
            State4 = op_span_finish(State3),
            {ok, State4}
    end;

do_save(_Reason, _SaveOpts, State) ->
    {ok, State}.


% ===================================================================
%% gen_server behaviour
%% ===================================================================


%% @private
-spec init(term()) ->
    {ok, state()} | {stop, term()}.

init({Op, ActorId, Actor, Config, SrvId, Caller, Ref}) ->
    % We create a top-level span for generic actor operations
    % Also, if Config has option 'ot_span_id' a specific operation span will be created
    % in do_pre_init
    State1 = do_pre_init(ActorId, Actor, Config, SrvId),
    State2 = op_span_check_create(Op, Config, State1),
    op_span_log(<<"starting initialization: ~p">>, [Op], State2),
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
    op_span_tags(Tags, State2),
    case do_check_expired(State2) of
        {false, State3} ->
            op_span_log(<<"registering with namespace">>, State3),
            case do_register(1, State3) of
                {ok, #actor_st{srv = SrvId} = State4} ->
                    op_span_update_srv_id(SrvId, State4),
                    op_span_log(<<"registered with namespace">>, State4),
                    case handle(actor_srv_init, [Op], State4) of
                        {ok, State5} ->
                            case do_post_init(Op, State5) of
                                {ok, State6} ->
                                    {ok, State6};
                                {error, Error} ->
                                    do_init_stop(Error, Caller, Ref, State5)
                            end;
                        {error, Error} ->
                            do_init_stop(Error, Caller, Ref, State4);
                        {delete, Error} ->
                            _ = do_delete(State4),
                            do_init_stop(Error, Caller, Ref, State4)
                    end;
                {error, Error} ->
                    do_init_stop(Error, Caller, Ref, State3)
            end;
        true ->
            op_span_log(<<"actor is expired on load">>, State2),
            ?ACTOR_LOG(warning, "actor is expired on load", [], State2),
            % Call stop functions, probably will delete the actor
            _ = do_stop(actor_expired, State2),
            do_init_stop(actor_not_found, Caller, Ref, State2)
    end.


%% @private
-spec handle_call(term(), {pid(), term()}, state()) ->
    {noreply, state()} | {reply, term(), state()} |
    {stop, Reason :: term(), state()} | {stop, Reason :: term(), Reply :: term(), state()}.

handle_call({nkactor_sync_op, Op}, From, State) ->
    case handle(actor_srv_sync_op, [Op, From], State) of
        {reply, Reply, #actor_st{} = State2} ->
            reply(Reply, do_refresh_ttl(State2));
        {reply_and_save, Reply, #actor_st{} = State2} ->
            {_, State3} = do_save(user_op, State2#actor_st{is_dirty = true}),
            reply(Reply, do_refresh_ttl(State3));
        {noreply, #actor_st{} = State2} ->
            noreply(do_refresh_ttl(State2));
        {noreply_and_save, #actor_st{} = State2} ->
            {_, State3} = do_save(user_op, State2#actor_st{is_dirty = true}),
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
    case handle(actor_srv_async_op, [Op], State) of
        {noreply, #actor_st{} = State2} ->
            noreply(do_refresh_ttl(State2));
        {noreply_and_save, #actor_st{} = State2} ->
            {_, State3} = do_save(user_op, State2#actor_st{is_dirty = true}),
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
    {_, State2} = do_save(hibernate, State),
    {noreply, State2, hibernate};

handle_cast(Msg, State) ->
    safe_handle(actor_srv_handle_cast, [Msg], State).


%% @private
-spec handle_info(term(), state()) ->
    {noreply, state()} | {stop, term(), state()}.

handle_info({nkactor_updated, _SrvId}, State) ->
    set_debug(State),
    noreply(State);

handle_info(nkactor_check_expire, State) ->
    case do_check_expired(State) of
        true ->
            do_stop(actor_expired, State);
        {false, State2} ->
            noreply(State2)
    end;

handle_info(nkactor_ttl_timeout, State) ->
    do_ttl_timeout(State);

handle_info(nkactor_timer_save, State) ->
    do_async_op({save, timer}, State);

handle_info(nkactor_check_next_status_time, State) ->
    case is_next_status_time(State) of
        {true, State2} ->
            {ok, State3} = handle(actor_srv_next_status_timer, [], State2),
            noreply(State3);
        {false, State2} ->
            noreply(State2)
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
            {ok, State3} = handle(actor_srv_link_down, [Link, Data], State2),
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
    {ok, _State3} = handle(actor_srv_terminate, [Reason], State2),
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

do_sync_op(get_actor, _From, #actor_st{actor=Actor}=State) ->
    {ok, UserActor, State2} = handle(actor_srv_get, [Actor], State),
    reply({ok, UserActor}, do_refresh_ttl(State2));

do_sync_op(consume_actor, From, #actor_st{actor=Actor}=State) ->
    {ok, UserActor, State2} = handle(actor_srv_get, [Actor], State),
    gen_server:reply(From, {ok, UserActor}),
    do_stop(actor_consumed, State2);

do_sync_op(get_state, _From, State) ->
    reply({ok, State}, State);

do_sync_op(get_timers, _From, State) ->
    #actor_st{ttl_timer=TTL, save_timer=Save, status_timer=Status} = State,
    Data = #{
        ttl => case is_reference(TTL) of true -> erlang:read_timer(TTL); _ -> 0 end,
        save => case is_reference(Save) of true -> erlang:read_timer(Save); _ -> 0 end,
        status => case is_reference(Status) of true -> erlang:read_timer(Status); _ -> 0 end
    },
    reply({ok, Data}, State);

do_sync_op(get_links, _From, State) ->
    Data = do_get_links(State),
    reply({ok, Data}, State);

do_sync_op(save, _From, State) ->
    {Reply, State2} = do_save(user_order, State),
    reply(Reply, do_refresh_ttl(State2));

do_sync_op(force_save, _From, State) ->
    {Reply, State2} = do_save(user_order, State#actor_st{is_dirty=true}),
    reply(Reply, do_refresh_ttl(State2));

do_sync_op(get_unload_policy, _From, #actor_st{unload_policy=Policy}=State) ->
    reply({ok, Policy}, State);

do_sync_op(delete, From, #actor_st{is_enabled=IsEnabled, config=Config}=State) ->
    case {IsEnabled, Config} of
        {false, #{dont_delete_on_disabled:=true}} ->
            reply({error, actor_is_disabled}, State);
        _ ->
            {Reply, State2} = do_delete(State),
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
            case do_update(UpdActor, #{}, State) of
                {ok, State2} ->
                    reply(ok, do_refresh_ttl(State2));
                {error, Error, State2} ->
                    reply({error, Error}, State2)
            end
    end;

do_sync_op(is_enabled, _From, #actor_st{is_enabled=IsEnabled}=State) ->
    reply({ok, IsEnabled}, State);

do_sync_op({link, Link, Opts}, _From, State) ->
    State2 = do_add_link(Link, Opts, State),
    reply(ok, do_refresh_ttl(State2));

do_sync_op({update, Actor, Opts}, _From, State) ->
    #actor_st{is_enabled=IsEnabled, config=Config, actor=Actor} = State,
    case {IsEnabled, Config} of
        {false, #{dont_update_on_disabled:=true}} ->
            reply({error, actor_is_disabled}, State);
        _ ->
            case do_update(Actor, Opts, State) of
                {ok, State2} ->
                    ReplyActor = case Opts of
                        #{get_actor:=true} ->
                            Actor;
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

do_sync_op({set_alarm, Class, Body}, _From, State) ->
    reply(ok, do_add_alarm(Class, Body, State));

do_sync_op({apply, Mod, Fun, Args}, From, State) ->
    apply(Mod, Fun, Args++[From, do_refresh_ttl(State)]);

do_sync_op(Op, _From, State) ->
    ?ACTOR_LOG(notice, "unknown sync op: ~p", [Op], State),
    reply({error, unknown_op}, State).


%% @private
do_async_op({unlink, Link}, State) ->
    case do_remove_link(Link, State) of
        {true, State2} ->
            {ok, State2};
        false ->
            {ok, State}
    end;

do_async_op({send_info, Info, Meta}, State) ->
    noreply(do_event({info, nklib_util:to_binary(Info), Meta}, do_refresh_ttl(State)));

do_async_op({send_event, Event}, State) ->
    noreply(do_event(Event, do_refresh_ttl(State)));

do_async_op({set_next_status_time, Time}, State) ->
    noreply(do_set_next_status_time(Time, State));

do_async_op({set_dirty, IsDirty}, State) when is_boolean(IsDirty)->
    noreply(do_refresh_ttl(State#actor_st{is_dirty=IsDirty}));

do_async_op(save, State) ->
    do_async_op({save, user_order}, State);

do_async_op({save, Reason}, State) ->
    {_Reply, State2} = do_save(Reason, State),
    noreply(do_refresh_ttl(State2));

do_async_op(delete, #actor_st{is_enabled=IsEnabled, config=Config}=State) ->
    case {IsEnabled, Config} of
        {false, #{dont_delete_on_disabled:=true}} ->
            noreply(State);
        _ ->
            {_Reply, State2} = do_delete(State),
            do_stop(actor_deleted, State2)
    end;
do_async_op({stop, Reason}, State) ->
    ?ACTOR_DEBUG("received stop: ~p", [Reason], State),
    do_stop(Reason, State);

do_async_op({raw_stop, Reason}, State) ->
    % We don't send the deleted event here, since we may not be active at all
    ?ACTOR_LOG(warning, "received raw_stop: ~p", [Reason], State),
    {_, State2} = handle(actor_srv_stop, [Reason], State),
    State3 = do_event({stopped, Reason}, State2),
    {stop, normal, State3#actor_st{stop_reason=raw_stop}};

do_async_op({set_alarm, Class, Body}, State) ->
    noreply(do_add_alarm(Class, Body, State));

do_async_op(clear_all_alarms, State) ->
    noreply(do_clear_all_alarms(State));

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
    State = #actor_st{
        srv = SrvId,    % Provisional
        module = maps:get(module, Config),
        config = Config,
        actor_id = ActorId2,
        actor = Actor,
        run_state = undefined,
        links = nklib_links:new(),
        is_dirty = false,
        saved_metadata = Meta,
        is_enabled = maps:get(is_enabled, Meta, true),
        save_timer = undefined,
        activated_time = nklib_date:epoch(usecs),
        unload_policy = permanent,           % Updated later
        op_span_ids = []
    },
    case maps:is_key(next_status_time, Meta) of
        true ->
            self() ! nkactor_check_next_status_time;
        false ->
            ok
    end,
    set_unload_policy(State).


%% @private
do_post_init(Op, State) ->
    #actor_st{config=Config} = State,
    ?ACTOR_DEBUG("started (~p)", [self()], State),
    State2 = case Op of
        create ->
            op_span_log(<<"starting creation save">>, State),
            State#actor_st{is_dirty=true};
        _ ->
            State
    end,
    case do_save(Op, State2) of
        {ok, State3} ->
            State4 = case Op of
                create ->
                    op_span_log(<<"actor saved">>, State),
                    do_event(created, State3);
                _ ->
                    op_span_log(<<"actor activated">>, State),
                    do_event(activated, State3)
            end,
            State5 = do_check_alarms(State4),
            case Config of
                #{heartbeat_time:=HeartbeatTime} ->
                    erlang:send_after(HeartbeatTime, self(), nkactor_heartbeat);
                _ ->
                    ok
            end,
            nklib_counters:async([?MODULE]),
            op_span_log(<<"actor init completed">>, State5),
            State6 = op_span_finish(State5),
            {ok, do_refresh_ttl(State6)};
        {{error, Error}, State3} ->
            op_span_log("actor save error: ~p", [Error], State3),
            {error, Error}
    end.


%% @private
do_init_stop(Error, Caller, Ref, State) ->
    op_span_log("actor init error: ~p", [Error], State),
    op_span_error(Error, State),
    _State2 = op_span_finish(State),
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
set_unload_policy(#actor_st{actor=Actor, config=Config}=State) ->
    #{metadata:=Meta} = Actor,
    ExpiresTime = maps:get(expires_time, Meta, <<>>),
    Policy = case Config of
        #{permanent:=true} ->
            permanent;
        _ when ExpiresTime /= <<>> ->
            {ok, Expires2} = nklib_date:to_epoch(ExpiresTime, msecs),
            % self() ! nkactor_check_expire,
            {expires, Expires2};
        #{auto_activate:=true} ->
            permanent;
        _ ->
            % A TTL reseated after each operation
            TTL = maps:get(ttl, Config, ?DEFAULT_TTL),
            ?ACTOR_DEBUG("TTL is ~p", [TTL], State),
            {ttl, TTL}
    end,
    ?ACTOR_DEBUG("unload policy is ~p", [Policy], State),
    State#actor_st{unload_policy=Policy}.


%% @private
do_register(Tries, #actor_st{actor_id=ActorId} = State) ->
    case nkactor_namespace:register_actor(ActorId) of
        {ok, SrvId, Pid} ->
            State2 = State#actor_st{srv=SrvId, namespace_pid=Pid},
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
            {ok, State2} = handle(actor_srv_alarms, [], State),
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
do_check_expired(#actor_st{unload_policy={expires, Expires}, ttl_timer=Timer}=State) ->
    nklib_util:cancel_timer(Timer),
    case nklib_date:epoch(msecs) of
        Now when Now >= Expires ->
            true;
        Now ->
            Remind = min(3600000, Expires - Now),
            Ref = erlang:send_after(Remind, self(), nkactor_check_expire),
            {false, State#actor_st{ttl_timer=Ref}}
    end;

do_check_expired(State) ->
    {false, State}.


%% @private
do_stop(Reason, State) ->
    {stop, normal, do_stop2(Reason, State)}.


%% @private
do_stop2(Reason, #actor_st{stop_reason=false}=State) ->
    State2 = State#actor_st{stop_reason=Reason},
    State3 = do_event({stopped, Reason}, State2),
    case handle(actor_srv_stop, [Reason], State3) of
        {ok, State4} ->
            {_, State5} = do_save(unloaded, State4),
            State5;
        {delete, State4} ->
            {_, State5} = do_delete(State4),
            State5
    end;

do_stop2(_Reason, State) ->
    State.


%% @private
do_enabled(Enabled, #actor_st{is_enabled=Enabled}=State) ->
    State;

do_enabled(Enabled, State) ->
    State2 = State#actor_st{is_enabled = Enabled},
    {ok, State3} = handle(actor_srv_enabled, [Enabled], State2),
    do_event({enabled, Enabled}, State3).


%% @private
do_heartbeat(State) ->
    case handle(actor_srv_heartbeat, [], State) of
        {ok, State2} ->
            noreply(State2);
        {error, Error} ->
            do_stop(Error, State)
    end.


%% @private
do_add_alarm(Class, Body, #actor_st{actor=Actor}=State) when is_map(Body) ->
    #{metadata:=Meta} = Actor,
    Alarms = maps:get(in_alarm, Meta, #{}),
    Body2 = case maps:is_key(last_time, Body) of
        true ->
            Body;
        false ->
            Body#{last_time => nklib_date:now_3339(secs)}
    end,
    Alarms2 = Alarms#{nklib_util:to_binary(Class) => Body2},
    Meta2 = Meta#{in_alarm => true, alarms => Alarms2},
    Actor2 = Actor#{metadata:=Meta2},
    State2 = State#actor_st{actor=Actor2, is_dirty=true},
    do_refresh_ttl(do_event({alarm_fired, Class, Body}, State2)).


%% @private
do_clear_all_alarms(#actor_st{actor=Actor}=State) ->
    #{metadata := Meta} = Actor,
    case Meta of
        #{in_alarm:=true} ->
            Meta2 = maps:without([in_alarm, alarms], Meta),
            Actor2 = Actor#{metadata := Meta2},
            State2 = State#actor_st{actor=Actor2, is_dirty=true},
            do_refresh_ttl(do_event(alarms_all_cleared, State2));
        _ ->
            State
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
do_ttl_timeout(#actor_st{unload_policy={ttl, _}, links=Links, status_timer=undefined}=State) ->
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
is_next_status_time(State) ->
    #actor_st{actor=Actor, status_timer=Timer1} = State,
    case Actor of
        #{metadata:=#{next_status_time:=NextTime}=Meta} ->
            nklib_util:cancel_timer(Timer1),
            Now = nklib_date:epoch(msecs),
            {ok, NextTime2} = nklib_date:to_epoch(NextTime, msecs),
            case NextTime2 - Now of
                Step when Step=<0 ->
                    Meta2 = maps:remove(next_status_time, Meta),
                    State2 = State#actor_st{
                        status_timer = undefined,
                        actor = Actor#{metadata:=Meta2},
                        is_dirty = true
                    },
                    {true, State2};
                Step ->
                    Step2 = min(Step, ?MAX_STATUS_TIME),
                    Timer2 = erlang:send_after(Step2, self(), nkactor_check_next_status_time),
                    State2 = State#actor_st{status_timer = Timer2},
                    {false, State2}
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
%% Will call the service's functions
handle(Fun, Args, State) ->
    #actor_st{srv=SrvId} = State,
    %lager:error("NKLOG CALL ~p:~p:~p", [SrvId, Fun, Args]),
    ?CALL_SRV(SrvId, Fun, Args++[State]).


%% @privateF
safe_handle(Fun, Args, State) ->
    Reply = handle(Fun, Args, State),
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
link_down(Mon, #actor_st{links=Links}=State) ->
    case nklib_links:down(Mon, Links) of
        {ok, Link, #link_info{data=Data}=LinkInfo, Links2} ->
            case LinkInfo of
                #link_info{gen_events=true, data=Data} ->
                    State2 = do_event({link_down, Data}, State),
                    {ok, Link, LinkInfo, State2#actor_st{links=Links2}};
                _ ->
                    {ok, Link, LinkInfo, State#actor_st{links=Links2}}
            end;
        not_found ->
            not_found
    end.


%% @private
op_span_check_create(Op, Config, #actor_st{srv=SrvId, op_span_ids=SpanIds}=ActorSt) ->
    case Config of
        #{ot_span_id:=ParentSpan} when ParentSpan /= undefined ->
            Name = <<"ActorSrv::", (nklib_util:to_binary(Op))/binary>>,
            SpanId = nkserver_ot:new(Op, SrvId, Name, ParentSpan),
            ActorSt#actor_st{op_span_ids=[SpanId|SpanIds]};
        _ ->
            ActorSt
    end.


%% @private
op_span_force_create(Op, #actor_st{op_span_ids=SpanIds}=ActorSt) ->
    ParentSpan = case SpanIds of
        [SpanId|_] ->
            SpanId;
        [] ->
            {undefined, undefined}
    end,
    op_span_check_create(Op, #{ot_span_id=>ParentSpan}, ActorSt).




%% @private
op_span_finish(#actor_st{op_span_ids=[]}=ActorSt) ->
    ActorSt;

op_span_finish(#actor_st{op_span_ids=[SpanId|Rest]}=ActorSt) ->
    nkserver_ot:finish(SpanId),
    ActorSt#actor_st{op_span_ids=Rest}.



%% @private
op_span_log(Log, #actor_st{op_span_ids=[SpanId|_]}) ->
    nkserver_ot:log(SpanId, Log);

op_span_log(_Log, #actor_st{op_span_ids=[]}) ->
    ok.


%% @private
op_span_log(Txt, Data, #actor_st{op_span_ids=[SpanId|_]}) ->
    nkserver_ot:log(SpanId, Txt, Data);

op_span_log(_Txt, _Data, #actor_st{op_span_ids=[]}) ->
    ok.



%% @private
op_span_tags(Tags, #actor_st{op_span_ids=[SpanId|_]}) ->
    nkserver_ot:tags(SpanId, Tags);

op_span_tags(_Tags, #actor_st{op_span_ids=[]}) ->
    ok.


%% @private
op_span_error(Error, #actor_st{op_span_ids=[SpanId|_]}) ->
    nkserver_ot:tag_error(SpanId, Error);

op_span_error(_Error, #actor_st{op_span_ids=[]}) ->
    ok.

%% @private
op_span_update_srv_id(SrvId, #actor_st{op_span_ids=[SpanId|_]}) ->
    nkserver_ot:update_srv_id(SpanId, SrvId);

op_span_update_srv_id(_SrvId, #actor_st{op_span_ids=[]}) ->
    ok.


%%%% @private
%%links_iter(Fun, Acc, #actor_st{links=Links}) ->
%%    nklib_links:fold_values(Fun, Acc, Links).

%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).
