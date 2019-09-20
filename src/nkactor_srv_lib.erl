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

-export([event/2, event_link/2, update/3, delete/1, set_activate/2, set_activate_time/2,
         set_expires_time/2, get_links/1, add_link/3, remove_link/2, save/2,
        remove_all_links/1, add_actor_event/4, set_dirty/1,
         update_status/2, update_status/3, add_actor_alarm/2, clear_all_alarms/1]).
-export([handle/3, set_unload_policy/1]).
-export([op_span_check_create/3, op_span_force_create/2, op_span_finish/1,
         op_span_log/2, op_span_log/3, op_span_tags/2, op_span_error/2]).

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver_ot/include/nkserver_ot.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(DEFAULT_TTL, 10000).

%% ===================================================================
%% In-process API
%% ===================================================================

%% @doc
event(Event, State) ->
    ?ACTOR_DEBUG("sending 'event': ~p", [Event], State),
    State2 = event_link(Event, State),
    {ok, State3} = handle(actor_srv_event, [Event], State2),
    State3.


%% @doc
event_link(Event, #actor_st{links = Links} = State) ->
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


%% @doc
set_activate(true, #actor_st{actor=Actor}=State) ->
    case Actor of
        #{metadata:=#{activate_time:=<<"1">>}} ->
            State;
        #{metadata:=Meta} ->
            Meta2 = Meta#{activate_time=><<"1">>},
            Actor2 = Actor#{metadata:=Meta2},
            set_dirty(State#actor_st{actor=Actor2})
    end;

set_activate(false, #actor_st{actor=Actor, activate_timer =Timer}=State) ->
    nklib_util:cancel_timer(Timer),
    #{metadata:=Meta} = Actor,
    case maps:is_key(activate_time, Meta) of
        true ->
            Meta2 = maps:remove(activate_time, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            set_dirty(State#actor_st{actor=Actor2});
        false ->
            State
    end.


%% @doc
set_activate_time(NextTime, #actor_st{actor=Actor}=State) ->
    {ok, NextTime2} = nklib_date:to_3339(NextTime, usecs),
    #{metadata:=Meta} = Actor,
    Meta2 = Meta#{activate_time => NextTime2},
    Actor2 = Actor#{metadata:=Meta2},
    self() ! nkactor_check_activate_time,
    set_dirty(State#actor_st{actor=Actor2}).


%% @doc
set_expires_time(Time, #actor_st{actor=Actor}=State) ->
    #{metadata:=Meta} = Actor,
    Meta2 = Meta#{expires_time => Time},
    set_unload_policy(State#actor_st{actor=Actor#{metadata:=Meta2}}).


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
            event({link_added, Link}, State);
        _ ->
            State
    end,
    State2#actor_st{links = nklib_links:add(Link, LinkInfo, Links)}.


%% @doc
remove_link(Link, #actor_st{links = Links} = State) ->
    case nklib_links:get_value(Link, Links) of
        {ok, #link_info{gen_events = true}} ->
            State2 = event({link_removed, Link}, State),
            State3 = State2#actor_st{links = nklib_links:remove(Link, Links)},
            {true, State3};
        not_found ->
            false
    end.


%% @doc
%% It will create an 'operation span' if ot_span_id option is used
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
                        throw({updated_namespace_invalid, UpdNamespace})
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
                    _ -> throw({updated_invalid_field, namespace})
                end,
                case UpdName of
                    Name -> ok;
                    _ -> throw({updated_invalid_field, name})
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
        MetaSyntax = #{
            vsn => binary,                  % Added by parse
            subtype => binary,
            is_enabled => boolean,
            expires_time => [date_3339, {binary, [<<>>]}],
            activate_time => [date_3339, {binary, [<<"1">>, <<>>]}],
            labels => #{'__map_binary' => binary},
            annotations => #{'__map_binary' => binary},
            links => #{'__map_binary' => binary},
            fts => #{'__map_binary' => binary},
            description => binary,
            updated_by => binary
        },
        UpdMeta2 = case nklib_syntax:parse(UpdMeta1, MetaSyntax, #{path=><<"metadata">>}) of
            {ok, UpdMeta2_0, []} ->
                UpdMeta2_0;
            {ok, _, [Field|_]} ->
                throw({updated_invalid_field, Field})
        end,
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
        case IsCoreUpdated orelse IsDataUpdated orelse IsMetaUpdated of
            true ->
                % At this point, we create main span and operation span
                State2 = op_span_check_create(update, Opts, State),
                %lager:error("NKLOG UPDATE Data:~p, Meta:~p", [IsDataUpdated, IsMetaUpdated]),
                NewActor1 = Actor#{
                    namespace => UpdNamespace,
                    name => UpdName,
                    data => NewData,
                    metadata := NewMeta3
                },
                case nkactor_lib:update_check_fields(NewActor1, State2) of
                    ok ->
                        op_span_log(<<"calling actor_srv_update">>, State2),
                        case handle(actor_srv_update, [NewActor1], State2) of
                            {ok, NewActor2, State3} ->
                                State4 = set_dirty(State3#actor_st{actor=NewActor2}),
                                NewEnabled = maps:get(is_enabled, NewMeta3, true),
                                State5 = enabled(NewEnabled, State4),
                                State6 = set_unload_policy(State5),
                                op_span_log(<<"calling do_save">>, State6),
                                case save(update, #{}, State6) of
                                    {ok, State7} ->
                                        nkserver_ot:log(<<"actor updated">>, State7),
                                        op_span_log(<<"actor updated">>, State7),
                                        State8 = op_span_finish(State7),
                                        {ok, event({updated, UpdActor}, State8)};
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
delete(#actor_st{is_dirty = deleted} = State) ->
    {ok, State};

delete(#actor_st{srv = SrvId, actor_id = ActorId, actor = Actor} = State) ->
    case handle(actor_srv_delete, [], State) of
        {ok, State2} ->
            case ?CALL_SRV(SrvId, actor_db_delete, [SrvId, ActorId, #{}]) of
                {ok, DbMeta} ->
                    ?ACTOR_DEBUG("object deleted: ~p", [DbMeta], State),
                    {ok, event(deleted, State2#actor_st{is_dirty = deleted})};
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
set_dirty(#actor_st{actor=Actor}=State) ->
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
    State3 = op_span_force_create(save, State2),
    #actor_st{op_span_ids=[SpanId|_]} = State3,
    {SaveFun, SaveOpts2} = case Reason of
        create ->
            NoCheckUnique = not maps:get(create_check_unique, Config, true),
            {
                actor_db_create,
                SaveOpts#{no_unique_check=>NoCheckUnique, ot_span_id=>SpanId}
            };
        _ ->
            {
                actor_db_update,
                SaveOpts#{last_metadata=>SavedMeta, ot_span_id=>SpanId}}
    end,
    op_span_log(<<"calling actor_srv_save">>, State3),
    case handle(actor_srv_save, [Actor], State3) of
        {ok, SaveActor, #actor_st{config = Config}=State4} ->
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
                                    nkactor:async_op(Self, {send_event, saved});
                                {error, Error} ->
                                    Span3 = nkserver_ot:log(Span2, "save error: ~p", [Error]),
                                    Span4 = nkserver_ot:tag_error(Span3, Error),
                                    nkserver_ot:finish(Span4)
                            end
                        end),
                    op_span_log(<<"launched asynchronous save: ~p">>, [Pid], State4),
                    % We must guess that the save is successful
                    #{metadata:=NewMeta} = Actor,
                    State5 = op_span_finish(State4),
                    {ok, State5#actor_st{saved_metadata = NewMeta, is_dirty = false}};
                _ ->

                    case ?CALL_SRV(SrvId, SaveFun, [SrvId, SaveActor, SaveOpts2]) of
                        {ok, DbMeta} ->
                            op_span_log(<<"actor saved">>, State4),
                            State5 = op_span_finish(State4),
                            ?ACTOR_DEBUG("save (~p) (~p)", [Reason, DbMeta], State5),
                            % The metadata of the updated actor is the new old metadata
                            % to check differences
                            #{metadata:=NewMeta} = Actor,
                            State6 = State5#actor_st{saved_metadata = NewMeta, is_dirty = false},
                            {ok, event(saved, State6)};
                        {error, not_implemented} ->
                            op_span_log(<<"save not implemented">>, State4),
                            State5 = op_span_finish(State4),
                            {{error, not_implemented}, State5};
                        {error, Error} ->
                            op_span_log(<<"save error: ~p">>, [Error], State4),
                            op_span_error(Error, State4),
                            State5 = op_span_finish(State4),
                            ?ACTOR_LOG(warning, "save error: ~p", [Error], State5),
                            {{error, Error}, State5}
                    end
            end;
        {ignore, State4} ->
            op_span_log(<<"save ignored">>, State4),
            State5 = op_span_finish(State4),
            {ok, State5}
    end;

save(_Reason, _SaveOpts, State) ->
    {ok, State}.


%% @doc
enabled(Enabled, #actor_st{is_enabled=Enabled}=State) ->
    State;

enabled(Enabled, State) ->
    State2 = State#actor_st{is_enabled = Enabled},
    {ok, State3} = handle(actor_srv_enabled, [Enabled], State2),
    nkactor_srv_lib:event({enabled, Enabled}, State3).


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
set_unload_policy(#actor_st{actor=Actor, config=Config}=State) ->
    #{metadata:=Meta} = Actor,
    ExpiresTime = maps:get(expires_time, Meta, <<>>),
    ActivateTime = maps:get(activate_time, Meta, <<>>),
    Policy = case Config of
        #{permanent:=true} ->
            permanent;
        #{auto_activate:=true} ->
            permanent;
        _ when ActivateTime == <<"1">> ->
            permanent;
        _ ->
            % A TTL reseated after each operation
            TTL = maps:get(ttl, Config, ?DEFAULT_TTL),
            ?ACTOR_DEBUG("TTL is ~p", [TTL], State),
            {ttl, TTL}
    end,
    ?ACTOR_DEBUG("unload policy is ~p", [Policy], State),
    State2 = State#actor_st{unload_policy=Policy},
    case ExpiresTime of
        <<>> ->
            case ActivateTime of
                <<>> ->
                    State2;
                _ ->
                    set_activate_time(ActivateTime, State2)
            end;
        _ ->
            set_activate_time(ExpiresTime, State2)
    end.


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
    Events2 = case length(Events1) >= MaxEvents of
        true ->
            lists:sublist(Events1, MaxEvents-1);
        false ->
            Events1
    end,
    Events3 = [Event3|Events2],
    Meta2 = Meta#{events => Events3},
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
            State2 = nkactor_srv_lib:set_dirty(State#actor_st{actor=Actor2}),
            {ok, event({alarm_fired, Alarm}, State2)};
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
            State2 = nkactor_srv_lib:set_dirty(State#actor_st{actor=Actor2}),
            nkactor_srv_lib:event(alarms_all_cleared, State2);
        _ ->
            State
    end.





%% ===================================================================
%% Internal
%% ===================================================================


%% @private
op_span_check_create(Op, Config, #actor_st{srv=SrvId, op_span_ids=SpanIds}=State) ->
    case Config of
        #{ot_span_id:=ParentSpan} when ParentSpan /= undefined ->
            Name = <<"ActorSrv::", (nklib_util:to_binary(Op))/binary>>,
            SpanId = nkserver_ot:new(Op, SrvId, Name, ParentSpan),
            State#actor_st{op_span_ids=[SpanId|SpanIds]};
        _ ->
            State
    end.


%% @private
op_span_force_create(Op, #actor_st{op_span_ids=SpanIds}=State) ->
    ParentSpan = case SpanIds of
        [SpanId|_] ->
            SpanId;
        [] ->
            {undefined, undefined}
    end,
    op_span_check_create(Op, #{ot_span_id=>ParentSpan}, State).




%% @private
op_span_finish(#actor_st{op_span_ids=[]}=State) ->
    State;

op_span_finish(#actor_st{op_span_ids=[SpanId|Rest]}=State) ->
    nkserver_ot:finish(SpanId),
    State#actor_st{op_span_ids=Rest}.



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
