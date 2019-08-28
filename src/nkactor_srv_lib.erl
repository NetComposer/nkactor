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

-export([event/2, event_link/2, update/3, delete/1, set_next_status_time/2,
         unset_next_status_time/1, get_links/1, add_link/3, remove_link/2,
         save/2, set_active/2, remove_all_links/1, add_actor_event/4, set_dirty/1,
         copy_status_fields/3]).
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
set_next_status_time(NextTime, #actor_st{actor = Actor} = State) ->
    {ok, NextTime2} = nklib_date:to_3339(NextTime, msecs),
    #{metadata:=Meta} = Actor,
    Meta2 = Meta#{next_status_time => NextTime2},
    Actor2 = Actor#{metadata:=Meta2},
    self() ! nkactor_check_next_status_time,
    set_dirty(State#actor_st{actor=Actor2}).


%% @doc
unset_next_status_time(#actor_st{actor = Actor, status_timer = Timer} = State) ->
    nklib_util:cancel_timer(Timer),
    #{metadata:=Meta} = Actor,
    case maps:is_key(next_status_time, Meta) of
        true ->
            Meta2 = maps:remove(next_status_time, Meta),
            Actor2 = Actor#{metadata:=Meta2},
            set_dirty(State#actor_st{actor=Actor2});
        false ->
            State
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
update(UpdActor, Opts, #actor_st{actor_id = ActorId, actor = Actor} = State) ->
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
        Data = maps:get(data, Actor, #{}),
        UpdData1 = maps:get(data, UpdActor, #{}),
%%        DataFields = maps:get(data_fields, Opts, all),
%%        UpdData2 = case DataFields of
%%            all ->
%%                UpdData1;
%%            _ ->
%%                maps:with(DataFields, UpdData1)
%%        end,
        NewData = case maps:get(do_patch, Opts, false) of
            false ->
                UpdData1;
            true ->
%%                Data2 = case DataFields of
%%                    all ->
%%                        Data1;
%%                    _ ->
%%                        maps:with(DataFields, Data1)
%%                end,
                nklib_util:map_merge(UpdData1, Data)
        end,
        IsDataUpdated = NewData /= Data,
        UpdMeta = maps:get(metadata, UpdActor),
        Meta = maps:get(metadata, Actor),
        CT = maps:get(creation_time, Meta),
        case maps:get(creation_time, UpdMeta, CT) of
            CT -> ok;
            _ -> throw({updated_invalid_field, metadata})
        end,
        Kind = maps:get(kind, Meta, <<>>),
        case maps:get(kind, Meta, <<>>) of
            Kind -> ok;
            _ -> throw({updated_invalid_field, kind})
        end,
        SubType = maps:get(subtype, Meta, <<>>),
        case maps:get(subtype, UpdMeta, <<>>) of
            SubType -> ok;
            _ -> throw({updated_invalid_field, subtype})
        end,
        Links = maps:get(links, Meta, #{}),
        UpdLinks1 = maps:get(links, UpdMeta, #{}),
        UpdLinks2 = maps:merge(Links, UpdLinks1),
        UpdMeta2 = case UpdLinks2 == Links of
            true ->
                UpdMeta;
            false ->
                case nkactor_lib:check_links(UpdLinks2) of
                    {ok, UpdLinks3} ->
                        UpdMeta#{links => UpdLinks3};
                    {error, Error} ->
                        throw(Error)
                end
        end,
        Labels = maps:get(labels, Meta, #{}),
        UpdLabels1 = maps:get(labels, UpdMeta2, #{}),
        UpdLabels2 = maps:merge(Labels, UpdLabels1),
        UpdMeta3 = case UpdLabels2 == Labels of
            true ->
                UpdMeta2;
            false ->
                UpdLabels3 = maps:filter(fun(_, V) -> V /= <<>> end, UpdLabels2),
                UpdMeta2#{labels => UpdLabels3}
        end,
        Annotations = maps:get(annotations, Meta, #{}),
        UpdAnnotations1 = maps:get(annotations, UpdMeta3, #{}),
        UpdAnnotations2 = maps:merge(Annotations, UpdAnnotations1),
        UpdMeta4 = case UpdAnnotations2 == Annotations of
            true ->
                UpdMeta3;
            false ->
                UpdAnnotations3 = maps:filter(fun(_, V) -> V /= <<>> end, UpdAnnotations2),
                UpdMeta3#{annotations => UpdAnnotations3}
        end,
        Enabled = maps:get(is_enabled, Meta, true),
        UpdEnabled = maps:get(is_enabled, UpdMeta4, true),
        UpdMeta5 = case maps:get(is_enabled, UpdMeta4, true) of
            true ->
                maps:remove(is_enabled, UpdMeta4);
            false ->
                UpdMeta4
        end,
        NewMeta = maps:merge(Meta, UpdMeta5),
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
                NewActor1 = Actor#{data=>NewData, metadata:=NewMeta2},
                case nkactor_lib:update_check_fields(NewActor1, State2) of
                    ok ->
                        op_span_log(<<"calling actor_srv_update">>, State2),
                        case handle(actor_srv_update, [NewActor1], State2) of
                            {ok, NewActor2, State3} ->
                                State4 = set_dirty(State3#actor_st{actor=NewActor2}),
                                State5 = enabled(UpdEnabled, State4),
                                State6 = set_unload_policy(State5),
                                self() ! nkactor_check_expired,
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
copy_status_fields(Actor, Fields, #actor_st{actor=OldActor}) ->
    Data = maps:get(data, Actor, #{}),
    Status = maps:get(status, Data, #{}),
    OldData = maps:get(data, OldActor, #{}),
    OldStatus = maps:get(status, OldData, #{}),
    OldStatus2 = maps:with(Fields, OldStatus),
    Status2 = maps:merge(Status, OldStatus2),
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


%% @doc Marks the actor as 'active'
%% This marks metadata.is_active as true and dirty
set_active(IsActive, State) ->
    #actor_st{actor = #{metadata:=Meta} = Actor} = State,
    case maps:get(is_active, Meta, false) of
        IsActive ->
            State;
        _ ->
            Meta2 = case IsActive of
                true ->
                    Meta#{is_active=>true};
                false ->
                    maps:remove(is_active, Meta)
            end,
            Actor2 = Actor#{metadata:=Meta2},
            State#actor_st{actor = Actor2, is_dirty = true}
    end.


%% @doc
set_dirty(#actor_st{actor=Actor}=State) ->
    Actor2 = nkactor_lib:update(Actor, nklib_date:now_3339(usecs)),
    State#actor_st{actor = Actor2, is_dirty = true}.


%% @doc
save(Reason, State) ->
    save(Reason, #{}, State).


%% @doc
save(Reason, SaveOpts, #actor_st{is_dirty = true} = State) ->
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
                                    nkactor_srv:async_op(Self, {send_event, saved});
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
    Policy = case Config of
        #{permanent:=true} ->
            permanent;
        _ when ExpiresTime /= <<>> ->
            {ok, Expires2} = nklib_date:to_epoch(ExpiresTime, msecs),
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
