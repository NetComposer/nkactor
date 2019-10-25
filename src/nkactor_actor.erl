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

-export([get_module/3, get_config/1, get_config/2, get_common_config/1]).
-export([parse/4, unparse/3, request/3]).

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").

%% ===================================================================
%% Callbacks definitions
%% @see functions actor_srv_... in nkactor_callbacks
%% ===================================================================

%-type id() :: nkactor:id().
-type actor() :: nkactor:actor().
-type actor_id() :: nkactor:actor_id().
-type config() :: nkactor:config().
-type request() :: nkactor:request().
-type response() :: nkactor_request:response().
-type verb() :: nkactor:verb().
-type actor_st() :: nkactor:actor_st().

-type continue() ::
    continue | {continue, list()}.


%% @doc Called to get the actor's config directly from the callback file
%% Use nkactor_util:get_module_config/2 to get a calculated config
-callback config() -> config().


%% @doc Called to validate an actor, on read, create and on update
%% Do not add labels, links or anything on metadata, do it on
%% init/2 or update/2
%% - read is used after reading object from db
%% - create is used on actor creation
%% - update is used on the provided user's version of the modification
%%
%% Request will be the one provided by caller to
%% nkactor:get_actor/2 or a barebones one (only srv)

-callback parse(read|create|update, map(), request()) ->
    continue | {ok, Actor::map()} | {syntax, nkactor:vsn(), nklib_syntax:syntax()} | {error, term()}.


%% @doc Called to process an incoming actor request
%% SrvId will be the service supporting the domain in Req
%% If not implemented, or 'continue' is returned, standard processing will apply
-callback request(verb(), SubRes::binary(), actor_id(), request()) ->
    response() | continue.


%% @doc Called when a new actor starts
%% For create, actor is saved after this call (is_dirty does not matter)
%% For start, it is saved only if is_dirty
-callback init(create|start, actor_st()) ->
    {ok, actor_st()} | {error, Reason::term()}.


%% @doc Called when an update is requested
%% NOTICE: the received actor is the actor provided by the user.
%% It is valid, but any previous existing data or metadata is not present, and must
%% be retrieved from actor_st.
%% For example, if we added something to status or links
%% the will be gone and must be inserted again here
%% @see nkactor_srv_lib:copy_status_fields/3
-callback update(nkactor:actor(), actor_st()) ->
    {ok, nkactor:actor(), actor_st()} | {ok, actor_st()} |
    {error, nkserver:status(), actor_st()}.


%% @doc Called when the actor is about to be deleted
-callback delete(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:status(), actor_st()}.


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


%% @doc Called when activate_timer is fired
-callback activated(Time::binary(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called when expire timer is fired
-callback expired(Time::binary(), actor_st()) ->
    {ok, actor_st()} | {delete, actor_st()} | continue().


%% @doc Called when an event is sent, for each registered process to the session
%% The events are 'erlang' events (tuples usually)
-callback link_event(nklib:link(), term(), nkactor_srv:event(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called when an object is enabled/disabled
-callback enabled(boolean(), actor_st()) ->
    {ok, actor_st()} | continue().


%% @doc Called on actor heartbeat (5 secs)
-callback heartbeat(actor_st()) ->
    {ok, actor_st()} | {error, nkserver:status(), actor_st()} | continue().


%% @doc Called when about to save
-callback save(actor(), actor_st()) ->
    {ok, actor(), actor_st()} | {ok, actor_st()} |
    {ignore, actor_st()} | continue().


%% @doc Called when a requests asks for the actor
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
    parse/3, request/4, save/2,
    init/2, get/2, update/2, delete/1, sync_op/3, async_op/2, enabled/2, heartbeat/1,
    event/2, link_event/4, activated/2, expired/2,
    handle_call/3, handle_cast/2, handle_info/2, stop/2, terminate/2]).


%% ===================================================================
%% Public
%% ===================================================================

%% @doc Gets the callback module for an actor resource or type
-spec get_module(nkserver:id(), nkactor:group(),
                 nkactor:resource()|{singular, binary()}|{camel, binary()}|{short, binary()}) ->
    module() | undefined.

get_module(SrvId, Group, Key) when is_atom(SrvId) ->
    nkserver:get_cached_config(SrvId, nkactor, {module, to_bin(Group), Key}).


%% @doc Used to get modified configuration for the service responsible
%% Config is cached in memory after first use
%% @doc Used to get run-time configuration for an actor type
-spec get_config(nkserver:id(),  module()) ->
    map().

get_config(SrvId, Module) when is_atom(SrvId), is_atom(Module) ->
    case catch nklib_util:do_config_get({nkactor_config, SrvId, Module}, undefined) of
        undefined ->
            Config = make_actor_config(SrvId, Module),
            nklib_util:do_config_put({nkactor_config, SrvId, Module}, Config),
            Config;
        Config when is_map(Config) ->
            Config
    end;

get_config(SrvId, #actor_id{group=Group, resource=Resource}) ->
    case get_module(SrvId, Group, Resource) of
        undefined ->
            {error, {resource_invalid, Group, Resource}};
        Module when is_atom(Module) ->
            {ok, SrvId, get_config(SrvId, Module)}
    end.


%% @doc Used to get run-time configuration for an actor type
-spec get_config(#actor_id{}) ->
    {ok, nkserver:id(), map()} | {error, term()}.

get_config(#actor_id{namespace=Namespace}=ActorId) ->
    case nkactor_namespace:find_service(Namespace) of
        {ok, SrvId} ->
            get_config(SrvId, ActorId);
        {error, Error} ->
            {error, Error}
    end.


%% @doc
get_common_config(SrvId) ->
    case nklib_util:do_config_get({nkactor_config, SrvId}, undefined) of
        undefined ->
            Filter1 = ?CALL_SRV(SrvId, actor_fields_filter, [[]]),
            Filter2 = [{to_bin(Field), true} || Field <- Filter1],
            Filter3 = maps:from_list(Filter2),
            Sort1 = ?CALL_SRV(SrvId, actor_fields_sort, [[]]),
            Sort2 = [{to_bin(Field), true} || Field <- Sort1],
            Sort3 = maps:from_list(Sort2),
            Type1 = maps:to_list(?CALL_SRV(SrvId, actor_fields_type, [#{}])),
            Type2 = [{to_bin(Field), Type} || {Field, Type} <- Type1],
            Type3 = maps:from_list(Type2),
            Static1 = ?CALL_SRV(SrvId, actor_fields_static, [[]]),
            Static2 = lists:usort([to_bin(Field) || Field <- Static1]),
            Data = #{
                fields_filter => Filter3,
                fields_sort => Sort3,
                fields_type => Type3,
                fields_static => Static2
            },
            nklib_util:do_config_put({nkactor_config, SrvId}, Data),
            Data;
        Data ->
            Data
    end.


%% @private
make_actor_config(SrvId, Module) ->
    Config1 = Module:config(),
    Config2 = ?CALL_SRV(SrvId, actor_config, [Config1#{module=>Module}]),
    #{resource:=Res1} = Config2,
    Res2 = to_bin(Res1),
    Singular = case Config2 of
        #{singular:=S0} ->
            to_bin(S0);
        _ ->
            nkactor_lib:make_singular(Res2)
    end,
    Camel = case Config2 of
        #{camel:=C0} ->
            to_bin(C0);
        _ ->
            nklib_util:to_capital(Singular)
    end,
    ShortNames = case Config2 of
        #{short_names:=SN} ->
            [to_bin(N) || N <- SN];
        _ ->
            []
    end,
    Verbs = case Config2 of
        #{verbs:=Vs} ->
            [binary_to_atom(to_bin(V), utf8) || V <- Vs];
        _ ->
            [get, list, create]
    end,
    Versions = maps:get(versions, Config2),
    #{
        fields_filter :=CommonFilter,
        fields_sort := CommonSort,
        fields_type := CommonType,
        fields_static := CommonStatic
    } = get_common_config(SrvId),
    FieldsFilter1 = maps:get(fields_filter, Config2, []),
    FieldsFilter2 = [{to_field(Field), true} || Field <- FieldsFilter1],
    FieldsFilter3 = maps:from_list(FieldsFilter2),
    FieldsSort1 = maps:get(fields_sort, Config2, []),
    FieldsSort2 = [{to_field(Field), true} || Field <- FieldsSort1],
    FieldsSort3 = maps:from_list(FieldsSort2),
    FieldsType1 = maps:to_list(maps:get(fields_type, Config2, #{})),
    FieldsType2 = [{to_field(Field), Value} || {Field, Value} <- FieldsType1],
    FieldsType3 = maps:from_list(FieldsType2),
    FieldsStatic1 = maps:get(fields_static, Config2, []),
    FieldsStatic2 = [to_field(Field) || Field <- FieldsStatic1],
    Config2#{
        resource := Res2,
        verbs => Verbs,
        versions => Versions,
        singular => Singular,
        camel => Camel,
        short_names => ShortNames,
        fields_filter => maps:merge(FieldsFilter3, CommonFilter),
        fields_sort => maps:merge(FieldsSort3, CommonSort),
        fields_type => maps:merge(FieldsType3, CommonType),
        fields_static => lists:usort(FieldsStatic2++CommonStatic)
    }.


%% @private Adds "data." at head it not there
to_field(Field) ->
    case to_bin(Field) of
        <<"data.", _/binary>> = Field2 ->
            Field2;
        <<"metadata.", _/binary>> = Field2 ->
            Field2;
        Other ->
            <<"data.", Other/binary>>
    end.


%% @doc Used to parse an actor, for an specific request
%% Each external resource (store or API) is responsible to present the
%% canonical form for the actor (maps, etc.)
%% Field 'vsn' in request will contain vsn, if:
%% - It is included in the request from the beginning (and not in actor itself)
%% - It is included in actor's body (metadata.vsn)

%% If ot_span_id is used, logs will be added
-spec parse(nkserver:id(), read|create|update, actor(), request()) ->
    {ok, actor()} | {error, nkserver:status()}.

parse(SrvId, Op, Actor, Req) ->
    Group = case Actor of
        #{group:=ActorGroup} ->
            ActorGroup;
        _ ->
            maps:get(group, Req)
    end,
    Res = case Actor of
        #{resource:=ActorRes} ->
            ActorRes;
        _ ->
            maps:get(resource, Req)
    end,
    % See nkactor_callback in nkactor_plugin
    SpanId = maps:get(ot_span_id, Req, undefined),
    nkserver_ot:log(SpanId, <<"calling actor parse">>),
    Req2 = maps:merge(#{srv=>SrvId}, Req),
    Args = [parse, Group, Res, [Op, Actor, Req2]],
    case ?CALL_SRV(SrvId, nkactor_callback, Args) of
        continue ->
            nkserver_ot:log(SpanId, <<"default syntax">>),
            {ok, Actor};
        {ok, Actor2} ->
            nkserver_ot:log(SpanId, <<"actor is custom parsed">>),
            {ok, Actor2};
        {syntax, Vsn, Syntax} when is_map(Syntax) ->
            nkserver_ot:log(SpanId, <<"actor has custom syntax">>),
            nkactor_lib:parse_actor_data(Op, Actor, Vsn, Syntax);
        {syntax, Vsn, Syntax, Actor2} when is_map(Syntax) ->
            nkserver_ot:log(SpanId, <<"actor has custom syntax and actor">>),
            nkactor_lib:parse_actor_data(Op, Actor2, Vsn, Syntax);
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing actor: ~p">>, [Error]),
            {error, Error}
    end.


%% @doc Used to transform an actor into an external representation
-spec unparse(nkserver:id(), actor(), request()) ->
    {ok, actor()} | {error, nkserver:status()}.

unparse(SrvId, Actor, Req) ->
    #{group:=Group, resource:=Res} = Actor,
    SpanId = maps:get(ot_span_id, Req, undefined),
    nkserver_ot:log(SpanId, <<"calling actor unparse">>),
    Req2 = maps:merge(#{srv=>SrvId}, Req),
    Args = [unparse, Group, Res, [Actor, Req2]],
    case ?CALL_SRV(SrvId, nkactor_callback, Args) of
        continue ->
            nkserver_ot:log(SpanId, <<"default syntax">>),
            {ok, Actor};
        {ok, Actor2} ->
            nkserver_ot:log(SpanId, <<"actor is custom parsed">>),
            {ok, Actor2};
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing actor: ~p">>, Error),
            {error, Error}
    end.


%% @doc Used to call the 'request' callback on an actor's module, in case
%% it has implemented it (to support specific requests)
%% If parse_id is used, logs will be added
-spec request(nkserver:id(), #actor_id{}, any()) ->
    response() | continue.

request(SrvId, ActorId, Req) ->
    #{verb:=Verb, subresource:=SubRes} = Req,
    #actor_id{group = Group, resource = Res} = ActorId,
    Req2 = maps:merge(#{srv=>SrvId}, Req),
    Args = [request, Group, Res, [Verb, SubRes, ActorId, Req2]],
    % See nkactor_callback in nkactor_plugin
    SpanId = maps:get(ot_span_id, Req, undefined),
    nkserver_ot:log(SpanId, <<"calling actor request">>),
    case ?CALL_SRV(SrvId, nkactor_callback, Args) of
        continue ->
            nkserver_ot:log(SpanId, <<"no specific action">>),
            continue;
        Other ->
            nkserver_ot:log(SpanId, <<"specific action return">>),
            Other
    end.


%% ===================================================================
%% Internal
%% ===================================================================
%%

%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).
