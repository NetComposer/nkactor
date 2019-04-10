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
-module(nkactor_actor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([config/1, parse/3, request/3]).

-include("nkactor.hrl").
-include("nkactor_debug.hrl").


%% ===================================================================
%% Callbacks definitions
%% @see functions actor_srv_... in nkactor_callbacks
%% ===================================================================

%-type id() :: nkactor:id().
-type actor() :: nkactor:actor().
-type subresource() :: nkactor:subresource().
-type actor_id() :: nkactor:actor_id().
-type config() :: nkactor:config().
-type request() :: nkactor:request().
-type response() :: nkactor:response().
-type verb() :: nkactor:verb().
%-type vsn() :: nkactor:vsn().
-type actor_st() :: nkactor:actor_st().

-type continue() ::
    continue | {continue, list()}.


%% @doc Called to get the actor's config directly from the callback file
%% Use nkactor_util:get_module_config/2 to get a calculated config
-callback config() -> config().


%% @doc Called to parse an actor from an external representation
-callback parse(actor(), request()) ->
    {ok, actor()} | {syntax, nklib_syntax:syntax()} | {error, term()}.


%% @doc Called to process an incoming API
%% SrvId will be the service supporting the domain in ApiReq
%% If not implemented, or 'continue' is returned, standard processing will apply
-callback request(verb(), subresource(), actor_id(), request()) ->
    response() | continue.


%% @doc Called when a new actor starts
-callback init(create|start, actor_st()) ->
    {ok, actor_st()} | {error, Reason::term()}.


%% @doc Called when
-callback update(nkactor:actor(), actor_st()) ->
    {ok, nkactor:actor(), actor_st()} | {error, nkserver:msg(), actor_st()}.


%% @doc Called when
-callback delete(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:msg(), actor_st()}.


%% @doc Called to process sync operations
-callback sync_op(term(), {pid(), reference()}, actor_st()) ->
    {reply, Reply::term(), actor_st()} | {reply_and_save, Reply::term(), actor_st()} |
    {noreply, actor_st()} | {noreply_and_save, actor_st()} |
    {stop, Reason::term(), Reply::term(), actor_st()} |
    {stop, Reason::term(), actor_st()} |
    continue().


%% @doc Called to process async operations
-callback async_op(term(), actor_st()) ->
    {noreply, actor_st()} | {noreply_and_save, actor_st()} |
    {stop, Reason::term(), actor_st()} |
    continue().


%% @doc Called when an event is sent inside the actor process
%% Can be used to launch API events, calling
-callback event(term(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called when next_status_timer is fired
-callback next_status_timer(actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called when an event is sent, for each registered process to the session
%% The events are 'erlang' events (tuples usually)
-callback link_event(nklib:link(), term(), nkactor_srv:event(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called when an object is enabled/disabled
-callback enabled(boolean(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called on actor heartbeat (5 secs)
-callback heartbeat(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:msg(), actor_st()} | continue().


%% @doc Called when about to save
-callback save(actor(), actor_st()) ->
    {ok, actor(), actor_st()} | {ignore, actor_st()} | continue().


%% @doc Called when
-callback get(actor(), actor_st()) ->
    {ok, actor(), actor_st()} | {error, nkactor_msg:msg(), actor_st()} | continue().


%% @doc
-callback handle_call(term(), {pid(), term()}, actor_st()) ->
    {reply, term(), actor_st()} | {noreply, actor_st()} |
    {stop, Reason::term(), Reply::term(), actor_st()} |
    {stop, Reason::term(), actor_st()} | continue().


%% @doc
-callback handle_cast(term(), actor_st()) ->
    {noreply, actor_st()} | {stop, term(), actor_st()} | continue().


%% @doc
-callback handle_info(term(), actor_st()) ->
    {noreply, actor_st()} | {stop, term(), actor_st()} | continue().


%% @doc
-callback stop(Reason::term(), actor_st()) ->
    {ok, actor_st()}.


%% @doc
-callback terminate(Reason::term(), actor_st()) ->
    {ok, actor_st()}.


%% @doc
-optional_callbacks([
    parse/2, request/4, save/2,
    init/2, get/2, update/2, delete/1, sync_op/3, async_op/2, enabled/2, heartbeat/1,
    event/2, link_event/4, next_status_timer/1,
    handle_call/3, handle_cast/2, handle_info/2, stop/2, terminate/2]).


%% ===================================================================
%% Actor Proxy
%% ===================================================================


%% @doc Used to call the 'config' callback on an actor's module
%% You normally will use nkdomain_util:get_config/2, as its has a complete set of fields
config(Module) ->
    #{resource:=Res1} = Config = Module:config(),
    Res2 = to_bin(Res1),
    Singular = case Config of
        #{singular:=S0} ->
            to_bin(S0);
        _ ->
            nkactor_lib:make_singular(Res2)
    end,
    Camel = case Config of
        #{camel:=C0} ->
            to_bin(C0);
        _ ->
            nklib_util:to_capital(Singular)
    end,
    ShortNames = case Config of
        #{short_names:=SN} ->
            [to_bin(N) || N <- SN];
        _ ->
            []
    end,
    Verbs = case Config of
        #{verbs:=Vs} ->
            [binary_to_atom(to_bin(V), utf8) || V <- Vs];
        _ ->
            [get, list, create]
    end,
    Versions = maps:get(versions, Config),
    FilterFields1 = maps:get(filter_fields, Config, []),
    FilterFields2 = [to_bin(Field) || Field <- FilterFields1++filter_fields()],
    SortFields1 = maps:get(sort_fields, Config, []),
    SortFields2 = [to_bin(Field) || Field <- SortFields1++sort_fields()],
    FieldType1 = maps:get(field_type, Config, #{}),
    FieldType2 = maps:to_list(maps:merge(FieldType1, field_type())),
    FieldType3 = maps:from_list([{to_bin(Field), Value} || {Field, Value} <- FieldType2]),
    FieldTrans1 = maps:to_list(maps:get(field_trans, Config, #{})),
    FieldTrans2 = maps:from_list([{to_bin(Field), Value} || {Field, Value} <- FieldTrans1]),
    StaticFields1 = maps:get(immutable_fields, Config, []),
    StaticFields2 = [to_bin(Field) || Field <- StaticFields1],
    Config#{
        module => Module,
        resource := Res2,
        verbs => Verbs,
        versions => Versions,
        singular => Singular,
        camel => Camel,
        short_names => ShortNames,
        filter_fields => lists:usort(FilterFields2),
        sort_fields => lists:usort(SortFields2),
        field_type => FieldType3,
        field_trans => FieldTrans2,
        immutable_fields => lists:usort(StaticFields2)
    }.



%% @doc Used to parse an actor, trying the module callback first
%% Actor should come with vsn
-spec parse(module(), actor(), any()) ->
    {ok, actor()} | {error, nkserver:msg()}.

parse(undefined, _Actor, _Request) ->
    error(module_undefined);

parse(Module, Actor, Request) ->
    SynSpec = case erlang:function_exported(Module, parse, 2) of
        true ->
            apply(Module, parse, [Actor, Request]);
        false ->
            {syntax, #{}}
    end,
    case SynSpec of
        {ok, Actor3} ->
            {ok, Actor3};
        {syntax, Syntax2} when is_map(Syntax2) ->
            nkactor_lib:parse_actor_data(Actor, Syntax2);
        {syntax, Syntax2, Actor3} when is_map(Syntax2) ->
            nkactor_lib:parse_actor_data(Actor3, Syntax2);
        {error, Error} ->
            {error, Error}
    end.



%% @doc Used to call the 'request' callback on an actor's module
-spec request(module(), #actor_id{}, any()) ->
    response() | continue.

request(undefined, _ActorId, _Request) ->
    error(module_undefined);

request(Module, ActorId, Request) ->
    Verb = maps:get(verb, Request),
    SubRes = maps:get(subresource, Request, []),
    case erlang:function_exported(Module, request, 4) of
        true ->
            Module:request(Verb, SubRes, ActorId, Request);
        false ->
            continue
    end.


%% ===================================================================
%% Common fields
%% ===================================================================


%% @doc Default filter fields
%% Must be sorted!
filter_fields() ->
    [
        group,
        resource,
        'group+resource',           % Maps to special group + resource
        vsn,
        name,
        namespace,
        path,
        groups,
        uid,
        'metadata.hash',
        'metadata.subtype',
        'metadata.creation_time',
        'metadata.update_time',
        'metadata.created_by',
        'metadata.updated_by',
        'metadata.expires_time',
        'metadata.generation',
        'metadata.is_active',
        'metadata.is_enabled',
        'metadata.in_alarm'
    ].


%% @doc Default sort fields
%% Must be sorted!
sort_fields() ->
    [
        group,
        resource,
        'group+resource',
        name,
        namespace,
        path,
        'metadata.subtype',
        'metadata.creation_time',
        'metadata.update_time',
        'metadata.created_by',
        'metadata.updated_by',
        'metadata.expires_time',
        'metadata.generation',
        'metadata.is_active',
        'metadata.is_enabled',
        'metadata.in_alarm'
    ].


%%%% @doc
%%field_trans() ->
%%    #{
%%        <<"apiGroup">> => <<"group">>,
%%        <<"kind">> => <<"data.kind">>,
%%        <<"metadata.uid">> => <<"uid">>,
%%        <<"metadata.name">> => <<"name">>
%%    }.


%% @doc Field value, applied after trans
field_type() ->
    #{
        'metadata.generation' => integer,
        'metadata.is_active' => boolean,
        'metadata.is_enabled' => boolean,
        'metadata.in_alarm' => boolean
    }.




%% ===================================================================
%% Internal
%% ===================================================================
%%
%%%% @private
%%call_actor(Fun, Args, #actor_st{module=Module}) ->
%%    %?TRACE(call_actor_1, #{'fun'=>Fun}),
%%    case erlang:function_exported(Module, Fun, length(Args)) of
%%        true ->
%%            %?TRACE(call_actor_2),
%%            R = apply(Module, Fun, Args),
%%            %?TRACE(call_actor_3),
%%            R;
%%        false ->
%%            continue
%%    end;
%%
%%call_actor(_Fun, _Args, _ActorSt) ->
%%    continue.



%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).
