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

%% @doc
-module(nkactor_srv_lib).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([new_span/4, new_actor_span/4, event/3]).
-export([event_link/3, update/3, delete/2, set_auto_activate/2, set_activate_time/2,
         set_expire_time/3, get_links/1, add_link/3, remove_link/2, save/2,
         remove_all_links/1, add_actor_event/2, add_actor_event/3, add_actor_event/4, set_updated/1,
         update_status/2, update_status/3, add_actor_alarm/2, clear_all_alarms/1]).
-export([handle/3, set_times/1]).
-import(nkserver_trace, [trace/1, trace/2, log/3]).
-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include_lib("nkserver/include/nkserver_trace.hrl").

-define(DEFAULT_TTL, 10000).

%% ===================================================================
%% In-process API
%% ===================================================================


new_span(SpanId, Fun, Opts, #actor_st{srv=SrvId, actor=Actor}) ->
    nkserver_trace:new_span(SrvId, SpanId, Fun, Opts#{actor=>Actor}).


new_actor_span(Type, Fun, Opts, #actor_st{actor_id=ActorId}=ActorSt) ->
    #actor_id{group=Group, resource=Res} = ActorId,
    new_span({nkactor_actor, Group, Res, Type}, Fun, Opts, ActorSt).


%% @doc
event(EventType, Meta, State) ->
    #actor_st{actor_id = #actor_id{group=Group, resource=Res}} = State,
    nkserver_trace:event(EventType, Meta),
    State2 = event_link(EventType, Meta, State),
    {ok, State3} = handle(actor_srv_event, [Group, Res, EventType, Meta], State2),
    State3.


%% @doc
event_link(EventType, EventMeta, #actor_st{links = Links} = State) ->
    nklib_links:fold_values(
        fun
            (Link, #link_info{get_events = true, data = Data}, Acc) ->
                {ok, Acc2} = handle(actor_srv_link_event, [Link, Data, EventType, EventMeta], Acc),
                Acc2;
            (_Link, _LinkOpts, Acc) ->
                Acc
        end,
        State,
        Links).


%% @doc
set_auto_activate(Bool, #actor_st{actor=#{metadata:=Meta}=Actor}=State) ->
    case maps:get(auto_activate, Meta, false) of
        Bool ->
            State;
        _ when Bool ->
            Meta2 = Meta#{auto_activate => true},
            Actor2 = Actor#{metadata:=Meta2},
            set_updated(State#actor_st{actor=Actor2});
        _ ->
            Meta2 = maps:remove(auto_activate, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            set_updated(State#actor_st{actor=Actor2})
    end.


%% @doc
set_activate_time(<<>>, #actor_st{actor=Actor, activate_timer=Timer}=State) ->
    nklib_util:cancel_timer(Timer),
    case Actor of
        #{metadata:=#{activate_time:=_}=Meta} ->
            Meta2 = maps:remove(activate_time, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            State#actor_st{actor=Actor2, is_dirty=true};
        _ ->
            State
    end;

set_activate_time(Time, #actor_st{actor=#{metadata:=Meta}=Actor, activate_timer=Timer}=State) ->
    case Meta of
        #{activate_time:=Time} ->
            % If we set up activation on init callback, the init process will call again here
            State;
        _ ->
            nklib_util:cancel_timer(Timer),
            {ok, Time1} = nklib_date:to_epoch(Time, usecs),
            Time2 = Time1 + nklib_util:rand(0, 999),
            {ok, Time3} = nklib_date:to_3339(Time2, usecs),
            % Make sure we have usecs resolution, and random usecs
            % To avoid it to be the same (probably 0) in several requests
            Actor2 = Actor#{metadata:=Meta#{activate_time => Time3}},
            self() ! nkactor_check_activate_time,
            State#actor_st{actor=Actor2, is_dirty=true}
    end.


%% @doc Sets an expiration date
%% If Activate, actor will be activated on that date to perform the deletion
set_expire_time(<<>>, _Activate, #actor_st{actor=Actor, expire_timer=Timer}=State) ->
    nklib_util:cancel_timer(Timer),
    case Actor of
        #{metadata:=#{expire_time:=_}=Meta} ->
            Meta2 = maps:remove(expire_time, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            set_updated(State#actor_st{actor=Actor2});
        _ ->
            State
    end;

set_expire_time(Time, Activate, #actor_st{actor=Actor}=State) when is_boolean(Activate)->
    {ok, Time2} = nklib_date:to_3339(Time, usecs),
    #{metadata:=Meta} = Actor,
    Actor2 = case Meta of
        #{expire_time:=Time} ->
            Actor;
        _ ->
            Actor#{metadata:=Meta#{expire_time => Time2}}
    end,
    self() ! nkactor_check_expire_time,
    State2 = set_updated(State#actor_st{actor=Actor2}),
    case Activate of
        true ->
            set_activate_time(Time, State2);
        false ->
            State2
    end.


%% @doc
get_links(#actor_st{links = Links}) ->
    nklib_links:fold_values(
        fun(Link, #link_info{data = Data}, Acc) -> [{Link, Data} | Acc] end,
        [],
        Links).


%% @doc
add_link(Link, Opts, #actor_st{links = Links} = State) ->
    LinkInfo = #link_info{
        get_events = maps:get(get_events, Opts, false),
        gen_events = maps:get(gen_events, Opts, true),
        avoid_unload = maps:get(avoid_unload, Opts, false),
        data = maps:get(data, Opts, undefined)
    },
    State2 = case LinkInfo#link_info.gen_events of
        true ->
            event(link_added, #{link=>Link}, State);
        _ ->
            State
    end,
    State2#actor_st{links = nklib_links:add(Link, LinkInfo, Links)}.


%% @doc
remove_link(Link, #actor_st{links = Links} = State) ->
    case nklib_links:get_value(Link, Links) of
        {ok, #link_info{gen_events = true}} ->
            State2 = event(link_removed, #{link=>Link}, State),
            State3 = State2#actor_st{links = nklib_links:remove(Link, Links)},
            {true, State3};
        not_found ->
            false
    end.


%% @doc
update(UpdActor, Opts, #actor_st{actor_id=ActorId, actor=Actor}=State) ->
    UpdActorId = nkactor_lib:actor_to_actor_id(UpdActor),
    #actor_id{
        uid = UID,
        namespace = Namespace,
        group = Class,
        resource = Res,
        name = Name
    } = ActorId,
    try
        case UpdActorId#actor_id.uid of
            undefined -> ok;
            UID -> ok;
            _ -> throw({updated_static_field, uid})
        end,
        case UpdActorId#actor_id.group of
            Class -> ok;
            _ -> throw({updated_static_field, group})
        end,
        case UpdActorId#actor_id.resource of
            Res -> ok;
            _ -> throw({updated_static_field, resource})
        end,
        UpdNamespace = UpdActorId#actor_id.namespace,
        UpdName = UpdActorId#actor_id.name,
        case Opts of
            #{allow_name_change:=true} ->
                case
                    UpdNamespace /= Namespace andalso
                        nkactor_namespace:find_service(UpdNamespace) of
                    false ->
                        ok;
                    {ok, _} ->
                        ?ACTOR_LOG(notice, "updated namespace ~s -> ~s", [Namespace, UpdNamespace]),
                        ok;
                    _ ->
                        throw({updated_namespace_static, UpdNamespace})
                end,
                ActorId2 = ActorId#actor_id{
                    namespace = UpdNamespace,
                    name = UpdName,
                    uid = undefined
                },
                case nkactor:find(ActorId2) of
                    {ok, _} ->
                        throw(updated_name_exists);
                    _ ->
                        ok
                end;
            _ ->
                case UpdNamespace of
                    Namespace -> ok;
                    _ -> throw({updated_static_field, namespace})
                end,
                case UpdName of
                    Name -> ok;
                    _ -> throw({updated_static_field, name})
                end
        end,
        IsCoreUpdated = (UpdNamespace /= Namespace) orelse (UpdName /= Name),
        case IsCoreUpdated of
            true ->
                nkactor:stop(self(), updated_name);
            false ->
                ok
        end,
        Data = maps:get(data, Actor, #{}),
        UpdData1 = maps:get(data, UpdActor, #{}),
        DoDataMerge = maps:get(merge_data, Opts, false),
        NewData = case DoDataMerge of
            false ->
                UpdData1;
            true ->
                nkactor_lib:map_merge(Data, UpdData1)
        end,
        IsDataUpdated = NewData /= Data,
        Meta = maps:get(metadata, Actor, #{}),
        UpdMeta1 = maps:get(metadata, UpdActor),
        MetaSyntax = nkactor_syntax:meta_syntax(),
        UpdMeta2 = case nklib_syntax:parse(UpdMeta1, MetaSyntax, #{path=><<"metadata">>}) of
            {ok, UpdMeta2_0, []} ->
                UpdMeta2_0;
            {ok, _, [Field|_]} ->
                throw({updated_static_field, Field})
        end,
        % We will check later that no important fields has been modified
        NewMeta1 = nkactor_lib:map_merge(Meta, UpdMeta2),
        Links1 = maps:get(links, Meta, #{}),
        Links2 = maps:get(links, NewMeta1, #{}),
        NewMeta2 = case Links1 == Links2 of
            true ->
                NewMeta1;
            false ->
                case nkactor_lib:check_links(Links2) of
                    {ok, Links3} ->
                        NewMeta1#{links => Links3};
                    {error, Error} ->
                        throw(Error)
                end
        end,
        NewMeta3 = case NewMeta2 of
            #{is_enabled:=true} ->
                maps:remove(is_enabled, NewMeta2);
            _ ->
                NewMeta2
        end,
        IsMetaUpdated = (Meta /= NewMeta3),
        Forced = maps:get(force_update, Opts, false),
        case IsCoreUpdated orelse IsDataUpdated orelse IsMetaUpdated orelse Forced of
            true ->
                % At this point, we create main span and operation span
                Fun = fun() ->
                    NewActor1 = Actor#{
                        namespace => UpdNamespace,
                        name => UpdName,
                        data => NewData,
                        metadata := NewMeta3
                    },
                    case nkactor_lib:update_check_fields(NewActor1, State) of
                        ok ->
                            trace("calling actor_srv_update"),
                            case handle(actor_srv_update, [NewActor1, Opts], State) of
                                {ok, NewActor2, State2} ->
                                    State3 = set_updated(State2#actor_st{actor=NewActor2}),
                                    NewEnabled = maps:get(is_enabled, NewMeta3, true),
                                    State4 = enabled(NewEnabled, State3),
                                    State5 = set_times(State4),
                                    trace("calling do_save"),
                                    case save(update, #{}, State5) of
                                        {ok, State6} ->
                                            {ok, event(updated, #{update=>UpdActor}, State6)};
                                        {{error, SaveError}, State6} ->
                                            log(warning, "update save error: ~p", [SaveError]),
                                            nkserver_trace:error(SaveError),
                                            {error, SaveError, State6}
                                    end;
                                {error, UpdError, State2} ->
                                    trace("update error: ~p", [UpdError]),
                                    nkserver_trace:error(UpdError),
                                    {error, UpdError, State2}
                            end;
                        {error, StaticFieldError} ->
                            nkserver_trace:error(StaticFieldError),
                            {error, StaticFieldError, State}
                    end
                end,
                SpanOpts = maps:with([parent], Opts),
                new_span({nkactor_server, update}, Fun, SpanOpts, State);
            false ->
                {ok, State}
        end
    catch
        throw:Throw ->
            {error, Throw, State}
    end.


%% @doc Copy fields from 'data.status' from old actor to new
update_status(Actor, #actor_st{actor=OldActor}) ->
    Data = maps:get(data, Actor, #{}),
    OldData = maps:get(data, OldActor, #{}),
    Status = maps:get(status, OldData, #{}),
    Actor#{data=>Data#{status=>Status}}.


%% @doc Allow some fields on update status, copy rest from old
update_status(Actor, Fields, #actor_st{actor=OldActor}) ->
    Data = maps:get(data, Actor, #{}),
    UserStatus1 = maps:get(status, Data, #{}),
    UserStatus2 = maps:with(Fields, UserStatus1),
    OldData = maps:get(data, OldActor, #{}),
    OldStatus = maps:get(status, OldData, #{}),
    Status2 = maps:merge(OldStatus, UserStatus2),
    Actor#{data=>Data#{status=>Status2}}.


%% @doc
delete(_Opts, #actor_st{is_dirty = deleted} = State) ->
    {ok, State};

delete(Opts, #actor_st{srv = SrvId, actor_id = ActorId, actor = Actor} = State) ->
    case handle(actor_srv_delete, [Opts], State) of
        {ok, State2} ->
            case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, ActorId, #{}]) of
                {ok, DbMeta} ->
                    ?ACTOR_DEBUG("object deleted: ~p", [DbMeta], State),
                    {ok, event(deleted, #{}, State2#actor_st{is_dirty = deleted})};
                {error, Error} ->
                    case Error of
                        actor_has_linked_actors ->
                            ok;
                        _ ->
                            ?ACTOR_LOG(warning, "object could not be deleted: ~p", [Error], State)
                    end,
                    {{error, Error}, State2#actor_st{actor = Actor}}
            end;
        {error, Error, State2} ->
            {{error, Error}, State2}
    end.


%% @doc
set_updated(#actor_st{actor=Actor}=State) ->
    Actor2 = nkactor_lib:update(Actor, nklib_date:now_3339(usecs)),
    State#actor_st{actor = Actor2, is_dirty = true}.


%% @doc
save(Reason, State) ->
    save(Reason, #{}, State).


%% @doc
save(Reason, SaveOpts, #actor_st{actor=Actor, is_dirty = true} = State) ->
    #actor_st{
        srv = SrvId,
        actor = Actor,
        saved_metadata = SavedMeta,
        save_timer = Timer,
        config = Config
    } = State,
    nklib_util:cancel_timer(Timer),
    State2 = State#actor_st{save_timer = undefined},
    {SaveFun, SaveOpts2} = case Reason of
        create ->
            NoCheckUnique = not maps:get(create_check_unique, Config, true),
            {
                actor_db_create,
                SaveOpts#{no_unique_check=>NoCheckUnique}
            };
        _ ->
            {
                actor_db_update,
                SaveOpts#{last_metadata=>SavedMeta}}
    end,
    Fun = fun() ->
        trace("calling actor_srv_save"),
        case handle(actor_srv_save, [Actor], State2) of
            {ok, SaveActor, #actor_st{config = Config}=State3} ->
                case Config of
                    #{async_save:=true} when Reason /= create ->
                        Self = self(),
                        Spawn = fun() ->
                            Fun2 = fun() ->
                                trace("calling ~s", [SaveFun]),
                                case ?CALL_SRV(SrvId, SaveFun, [SrvId, SaveActor, SaveOpts2]) of
                                    {ok, _DbMeta} ->
                                        nkactor:async_op(Self, {send_event, saved});
                                    {error, Error} ->
                                        log(warning, "async save error: ~p", [Error]),
                                        nkserver_trace:error(Error)
                                end
                            end,
                            new_span({nkactor_server, async_save}, Fun2, #{}, State3)
                        end,
                        Pid = spawn_link(Spawn),
                        trace("launched asynchronous save: ~p", [Pid]),
                        % We must guess that the save is successful
                        #{metadata:=NewMeta} = Actor,
                        {ok, State3#actor_st{saved_metadata = NewMeta, is_dirty = false}};
                    _ ->
                        trace("calling ~s", [SaveFun]),
                        case ?CALL_SRV(SrvId, SaveFun, [SrvId, SaveActor, SaveOpts2]) of
                            {ok, DbMeta} ->
                                #{metadata:=NewMeta} = Actor,
                                State4 = State3#actor_st{saved_metadata = NewMeta, is_dirty = false},
                                {ok, event(saved, #{reason=>Reason, db_meta=>DbMeta}, State4)};
                            {error, not_implemented} ->
                                trace("save not implemented"),
                                {{error, not_implemented}, State3};
                            {error, Error} when Error==uniqueness_violation; Error==duplicated_name ->
                                trace("uniqueness violation"),
                                {{error, actor_already_exists}, State3};
                            {error, Error} ->
                                log(warning, "actor save error: ~p", [Error]),
                                nkserver_trace:error(Error),
                                {{error, Error}, State3}
                        end
                end;
            {ignore, State3} ->
                trace("save ignored"),
                {ok, State3}
        end
    end,
    SpanOpts = maps:with([parent], SaveOpts),
    new_span({nkactor_server, save}, Fun, SpanOpts, State);

save(_Reason, _SaveOpts, State) ->
    {ok, State}.


%% @doc
enabled(Enabled, #actor_st{is_enabled=Enabled}=State) ->
    State;

enabled(Enabled, State) ->
    State2 = State#actor_st{is_enabled = Enabled},
    {ok, State3} = handle(actor_srv_enabled, [Enabled], State2),
    nkactor_srv_lib:event(enabled, #{enabled=>Enabled}, State3).


%% @doc
remove_all_links(#actor_st{actor=#{metadata:=Metadata}=Actor}=ActorSt) ->
    Links1 = maps:get(links, Metadata),
    Links2 = maps:map(fun(_K, _V) -> <<>> end, Links1),
    Actor2 = Actor#{metadata := Metadata#{links := Links2}},
    nkactor_srv_lib:update(Actor2, #{}, ActorSt).


%% @doc
%% Will call the service's functions
handle(Fun, Args, State) ->
    #actor_st{srv=SrvId} = State,
    %lager:error("NKLOG CALL ~p:~p:~p", [SrvId, Fun, Args]),
    ?CALL_SRV(SrvId, Fun, Args++[State]).


%% @doc
set_times(State) ->
    #actor_st{
        actor = #{metadata:=Meta},
        activate_timer = ActivateTimer,
        expire_timer = ExpireTimer
    } = State,
    nklib_util:cancel_timer(ActivateTimer),
    nklib_util:cancel_timer(ExpireTimer),
    case maps:get(activate_time, Meta, <<>>) of
        <<>> ->
            ok;
        _ ->
            self() ! nkactor_check_activate_time
    end,
    case maps:get(expire_time, Meta, <<>>) of
        <<>> ->
            ok;
        _ ->
            self() ! nkactor_check_expire_time
    end,
    set_unload_policy(State).


%% @doc
add_actor_event(Class, ActorSt) ->
    add_actor_event(Class, <<>>, #{}, ActorSt).


%% @doc
add_actor_event(Class, Data, ActorSt) when is_map(Data) ->
    add_actor_event(Class, <<>>, Data, ActorSt);

add_actor_event(Class, Type, ActorSt) ->
    add_actor_event(Class, Type, #{}, ActorSt).


%% @doc
add_actor_event(Class, Type, Data, ActorSt) ->
    #actor_st{actor=#{metadata:=Meta}=Actor, config=Config} = ActorSt,
    Event1 = #{
        class => nklib_util:to_binary(Class),
        time => nklib_date:now_3339(usecs)
    },
    Event2 = case nklib_util:to_binary(Type) of
        <<>> ->
            Event1;
        <<"undefined">> ->
            Event1;
        <<"none">> ->
            Event1;
        Type2 ->
            Event1#{type => Type2}
    end,
    Event3 = case map_size(Data) of
        0 ->
            Event2;
        _ ->
            Event2#{data => Data}
    end,
    Events1 = maps:get(events, Meta, []),
    MaxEvents = maps:get(max_actor_events, Config, 10),
    Events2 = nklib_util:add_to_list(Event3, MaxEvents, Events1),
    Meta2 = Meta#{events => Events2},
    Actor2 = Actor#{metadata:=Meta2},
    % We don't call set_dirty, the actor is no really modified by user,
    % and many actors can insert a 'created' event, and would start with generation=1, etc.
    ActorSt#actor_st{actor=Actor2, is_dirty=true}.


%% @private
add_actor_alarm(Alarm, #actor_st{actor=Actor, config=Config}=State) ->
    Syntax = nkactor_syntax:alarm_syntax(),
    case nklib_syntax:parse(Alarm, Syntax) of
        {ok, #{class:=Class}=Alarm2, _} ->
            Alarm3 = case maps:is_key(last_time, Alarm) of
                true ->
                    Alarm2;
                false ->
                    Alarm2#{last_time => nklib_date:now_3339(usecs)}
            end,
            #{metadata:=Meta} = Actor,
            Alarms = maps:get(alarms, Meta, []),
            Alarms2 = [A || A <- Alarms, maps:get(class, A) /= Class],
            MaxAlarms = maps:get(max_actor_alarms, Config, 10),
            Alarms3 = case length(Alarms2) >= MaxAlarms of
                true ->
                    lists:sublist(Alarms2, MaxAlarms-1);
                false ->
                    Alarms2
            end,
            Alarms4 = [Alarm3 | Alarms3],
            Meta2 = Meta#{in_alarm => true, alarms => Alarms4},
            Actor2 = Actor#{metadata:=Meta2},
            State2 = nkactor_srv_lib:set_updated(State#actor_st{actor=Actor2}),
            {ok, event(alarm_fired, #{alarm=>Alarm}, State2)};
        {error, Error} ->
            {error, Error}
    end.


%% @private
clear_all_alarms(#actor_st{actor=Actor}=State) ->
    #{metadata := Meta} = Actor,
    case Meta of
        #{in_alarm:=true} ->
            Meta2 = maps:without([in_alarm, alarms], Meta),
            Actor2 = Actor#{metadata := Meta2},
            State2 = nkactor_srv_lib:set_updated(State#actor_st{actor=Actor2}),
            nkactor_srv_lib:event(alarms_all_cleared, #{}, State2);
        _ ->
            State
    end.





%% ===================================================================
%% Internal
%% ===================================================================

%% @private
set_unload_policy(#actor_st{actor=Actor, config=Config}=State) ->
    case Config of
        #{permanent:=true} ->
            ?ACTOR_DEBUG("unload policy is config permanent", [], State),
            State#actor_st{unload_policy=permanent};
        #{auto_activate:=true} ->
            ?ACTOR_DEBUG("unload policy is config auto_activate", [], State),
            State2 = nkactor_srv_lib:set_auto_activate(true, State),
            State2#actor_st{unload_policy=permanent};
        _ ->
            case Actor of
                #{metadata:=#{auto_activate:=true}} ->
                    ?ACTOR_DEBUG("unload policy is actor auto_activate", [], State),
                    State2 = nkactor_srv_lib:set_auto_activate(true, State),
                    State2#actor_st{unload_policy=permanent};
                _ ->
                    % A TTL reseated after each operation
                    TTL = maps:get(ttl, Config, ?DEFAULT_TTL),
                    ?ACTOR_DEBUG("unload policy is TTL ~p", [TTL], State),
                    State#actor_st{unload_policy={ttl, TTL}}
            end
    end.



