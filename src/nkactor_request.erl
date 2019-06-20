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

-module(nkactor_request).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([request/1]).
-export([get_watches/0]).
-export([create/3]).
-export_type([request/0, response/0]).

-include("nkactor_request.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(DELETE_COLLECTION_SIZE, 10000).


%% ===================================================================
%% Types
%% ===================================================================

-type request() ::
    #{
        verb => nkactor:verb(),
        group => nkactor:group(),
        namespace => nkactor:namespace(),
        resource => nkactor:resource(),
        name => nkactor:name(),
        uid => nkactor:uid(),
        subresource => nkactor:subresource(),
        params => #{binary() => binary()},
        body => term(),
        auth => map(),
        % If defined, will be used as parent of created span
        ot_span_id => nkserver_ot:span_id() | nkserver_ot:parent(),
        % Implementing nkdomain_api behaviour:
        callback => module(),
        % External url to use in callbacks:
        external_url => binary(),
        meta => map(),
        %% added by system:
        start_time => nklib_date:epoch(usecs),
        srv => nkserver:id()            % Service managing namespace
    }.


-type response() ::
    ok | {ok, map()} | {ok, map(), request()} |
    created | {created, map()} | {created, map(), request()} |
    {status, nkserver:msg()} |
    {status, nkserver:msg(), map()} |  %% See status/2
    {status, nkserver:msg(), map(), request()} |
    {error, nkserver:msg()} |
    {error, nkserver:msg(), request()}.





%% ===================================================================
%% Public
%% ===================================================================


%% @doc Launches an Request
%% - A new span will be created. If ot_span_id is defined, it will be used as parent
%% - Calls actor_authorize/1 for authorization of the request
%% - Finds service managing namespace, and adds srv and start_time


-spec request(request()) ->
    {ok|created, map(), request()} |
    {raw, {CT::binary(), Body::binary()}, request()} |
    {status|error, nkserver:msg(), request()}.

request(Req) ->
    ParentSpan = maps:get(ot_span_id, Req, undefined),
    nkserver_ot:new(?REQ_SPAN, undefined, <<"Actor::Request">>, ParentSpan),
    % Gets group and vsn from request or body
    case nkactor_syntax:parse_request(Req) of
        {ok, #{group:=Group, resource:=Res, namespace:=Namespace}=Req2} ->
            nkserver_ot:log(?REQ_SPAN, <<"request parsed">>),
            case nkactor_namespace:find_service(Namespace) of
                {ok, SrvId} ->
                    nkserver_ot:update_srv_id(?REQ_SPAN, SrvId),
                    nkserver_ot:log(?REQ_SPAN, <<"service found: ~s">>, [SrvId]),
                    nkserver_ot:tags(?REQ_SPAN, #{
                        <<"req.group">> => Group,
                        <<"req.resource">> => Res,
                        <<"req.namespace">> => Namespace,
                        <<"req.name">> => maps:get(name, Req2, <<>>),
                        <<"req.srv">> => SrvId
                    }),
                    % Add uid later
                    set_debug(SrvId),
                    ?REQ_DEBUG("incoming request for ~s", [Namespace]),
                    Req3 = Req2#{
                        srv => SrvId,
                        start_time => nklib_date:epoch(usecs)
                    },
                    nkserver_ot:log(?REQ_SPAN, <<"calling authorize">>),
                    case ?CALL_SRV(SrvId, actor_authorize, [Req3]) of
                        {true, Req5} ->
                            nkserver_ot:log(?REQ_SPAN, <<"request is authorized">>),
                            ?REQ_DEBUG("request is authorized", []),
                            Reply = do_request(Req5),
                            case reply(Reply, Req5) of
                                {raw, {CT, Bin}, Req6} ->
                                    nkserver_ot:log(?REQ_SPAN, "reply raw: ~s", [CT]),
                                    ?REQ_DEBUG("reply raw: ~p", [CT], Req6),
                                    nkserver_ot:finish(?REQ_SPAN),
                                    {raw, {CT, Bin}, Req6};
                                {Status, Data, Req6} ->
                                    nkserver_ot:log(?REQ_SPAN, "reply: ~p", [Reply]),
                                    ?REQ_DEBUG("reply: ~p", [Reply], Req6),
                                    nkserver_ot:finish(?REQ_SPAN),
                                    {Status, Data, Req6}
                            end;
                        false ->
                            nkserver_ot:log(?REQ_SPAN, <<"request is NOT authorized">>),
                            nkserver_ot:finish(?REQ_SPAN),
                            {error, unauthorized, Req3}
                    end;
                {error, Error} ->
                    nkserver_ot:delete(?REQ_SPAN),
                    {error, Error, Req}
            end;
        {error, Error} ->
            nkserver_ot:delete(?REQ_SPAN),
            {error, Error, Req}
    end.


%% @doc
reply(Op, Req) when Op==ok; Op==created; Op==error; Op==status ->
    {Op, #{}, Req};

reply({Op, Reply}, Req) when Op==ok; Op==created; Op==error; Op==status ->
    {Op, Reply, Req};

reply({raw, {CT, Bin}}, Req) ->
    {raw, {CT, Bin}, Req};

reply(Other, Req) ->
    ?REQ_LOG(error, "Invalid API response: ~p", [Other]),
    {error, internal_error, Req}.


%% @private
-spec do_request(nkactor_api:request()) ->
    nkactor_api:response().

do_request(Req) ->
    try
        #{
            verb := Verb,
            group := Group,
            resource := Res,
            namespace := Namespace,
            srv := SrvId
        } = Req,
        ?REQ_DEBUG("incoming '~p' ~s (~p)", [Verb, Res, Req]),
        nkserver_ot:log(?REQ_SPAN, <<"starting request processing">>),
        ActorId = #actor_id{
            group = Group,
            resource = Res,
            name = maps:get(name, Req, <<>>),
            namespace = Namespace
        },
        Config = case catch nkactor_actor:get_config(ActorId) of
            {ok, SrvId, Config0} ->
                Config0;
            {error, ConfigError} ->
                throw({error, ConfigError})
        end,
        #{verbs:=Verbs} = Config,
        case lists:member(Verb, Verbs) of
            true ->
                ok;
            false ->
                throw({error, verb_not_allowed})
        end,
        % Parses the 'params' key in Req
        Req2 = case nkactor_syntax:parse_params(Verb, Req) of
            {ok, ParsedReq} ->
                ParsedReq;
            {error, ParseError} ->
                throw({error, ParseError})
        end,
        SubRes = maps:get(subresource, Req2, []),
        nkserver_ot:log(?REQ_SPAN, <<"calling specific processing">>),
        case nkactor_actor:request(SrvId, ActorId, Req2) of
            continue when SubRes == [] ->
                nkserver_ot:log(?REQ_SPAN, <<"processing default">>),
                ?REQ_DEBUG("processing default", [], Req2),
                default_request(Verb, ActorId, Config, Req2);
            continue ->
                nkserver_ot:log(?REQ_SPAN, <<"invalid subresource: ~s">>, [SubRes]),
                ?REQ_LOG(warning, "Invalid subresource (~p)", [{Verb, SubRes, ActorId}]),
                {error, resource_invalid};
            {continue, #{resource:=Res2}=Req3} when Res2 /= Res ->
                nkserver_ot:log(?REQ_SPAN, <<"request updated. New resource: ~p">>, [Res2]),
                ?REQ_DEBUG("updated request", [], Req3),
                do_request(Req2);
            Other ->
                nkserver_ot:log(?REQ_SPAN, <<"processed specific">>),
                ?REQ_DEBUG("specific processing", [], Req2),
                Other
        end
    catch
        throw:Throw ->
            nkserver_ot:log(?REQ_SPAN, <<"processing error: ~p">>, [Throw]),
            ?REQ_LOG(notice, "processing error: ~p", [Throw], Req),
            Throw
    end.


%% @private
%% A specific actor resource has been identified in Config
%% Maybe it is a final actor or a resource
default_request(get, #actor_id{name=Name}=ActorId, Config, Req) when is_binary(Name) ->
    get(ActorId, Config, Req);

default_request(list, #actor_id{name=undefined}=ActorId, Config, Req) ->
    list(ActorId, Config, Req);

default_request(create, ActorId, Config, Req) ->
    create(ActorId, Config, Req);

default_request(update, #actor_id{name=Name}=ActorId, Config, Req) when is_binary(Name) ->
    update(ActorId, Config, Req);

default_request(delete, #actor_id{name=Name}=ActorId, Config, Req) when is_binary(Name) ->
    delete(ActorId, Config, Req);

default_request(deletecollection, #actor_id{name=undefined}=ActorId, Config, Req) ->
    delete_collection(ActorId, Config, Req);

%%default_request(watch, ActorId, Config, Req) ->
%%    watch(ActorId, Config, Req);

default_request(Verb, ActorId, _Config, _Req) ->
    ?REQ_LOG(warning, "Invalid resource.1 (~p)", [{Verb, ActorId}]),
    {error, resource_invalid}.



%% ===================================================================
%% Default Operations
%% ===================================================================


%% @doc Standard get
get(ActorId, Config, Req) ->
    Opts = set_activate_opts(Config, Req),
    nkserver_ot:log(?REQ_SPAN, "processing standard read (~p)", [Opts]),
    ?REQ_DEBUG("processing read ~p (~p)", [ActorId, Opts]),
    % request is included in case it could be used for specific parsing
    % when calling nkactor_actor:parse()
    case nkactor:get_actor(ActorId, Opts#{request=>Req}) of
        {ok, Actor} ->
            {ok, Actor};
        {error, ReadError} ->
            nkserver_ot:log(?REQ_SPAN, "error getting actor: ~p", [ReadError]),
            {error, ReadError}
    end.


%% @doc
list(ActorId, Config, Req) ->
    #{params:=Params} = Req,
    case nkdomain_core_search:type_search(ActorId, Config, Params) of
        {ok, ActorList, _Meta} ->
            {ok, ActorList};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
create(ActorId, Config, Req) ->
    case parse_body(Req) of
        {ok, Actor1} ->
            nkserver_ot:log(?REQ_SPAN, <<"body parsed">>),
            case check_actor(Actor1, ActorId) of
                {ok, Actor2} ->
                    Opts = set_activate_opts(Config, Req),
                    nkserver_ot:log(?REQ_SPAN, "processing standard create (~p)", [Opts]),
                    ?REQ_DEBUG("creating actor ~p ~p", [Actor2, Opts]),
                    % request is included in case it could be used for specific parsing
                    % when calling nkactor_actor:parse()
                    case nkactor:create(Actor2, Opts#{get_actor=>true, request=>Req}) of
                        {ok, Actor3} ->
                            {created, Actor3};
                        {error, Error} ->
                            nkserver_ot:log(?REQ_SPAN, "error creating actor: ~p", [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    nkserver_ot:log(?REQ_SPAN, <<"error checking actor: ~p">>, [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(?REQ_SPAN, <<"error parsing body: ~p">>, [Error]),
            {error, Error}
    end.



%% @doc
update(ActorId, Config, Req) ->
     case parse_body(Req) of
        {ok, Actor1} ->
            case check_actor(Actor1, ActorId) of
                {ok, Actor2} ->
                    Opts = set_activate_opts(Config, Req#{request=>Req}),
                    ?REQ_DEBUG("Updating actor ~p", [Actor2]),
                    case nkactor:update(ActorId, Actor2, Opts) of
                        {ok, Actor3} ->
                            {ok, Actor3};
                        {error, actor_not_found} ->
                            default_request(create, ActorId, Config, Req);
                        {error, {field_missing, name}} ->
                            default_request(create, ActorId, Config, Req);
                        {error, UpdateError} ->
                            {error, UpdateError}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
         {error, Error} ->
             {error, Error}
     end.


%% @doc
delete(ActorId, _Config, Req) ->
    Params = maps:get(params, Req, #{}),
    Opts = #{cascade => maps:get(cascade, Params, false)},
    ?REQ_DEBUG("processing delete ~p (~p)", [ActorId, Opts]),
    case nkactor:delete(ActorId, Opts) of
        ok ->
            {status, actor_deleted};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
delete_collection(ActorId, Config, #{srv:=SrvId}=Req) ->
    #{params:=Params} = Req,
    Params2 = maps:merge(#{size=>?DELETE_COLLECTION_SIZE}, Params),
    case nkdomain_core_search:search_params_id(SrvId, ActorId, Config, Params2) of
        {ok, ActorIds, _Meta} ->
            do_delete(SrvId, ActorIds, 0);
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_delete(SrvId, ActorIds, Num) ->
    case length(ActorIds) > 100 of
        true ->
            {Set1, Set2} = lists:split(100, ActorIds),
            case nkdomain_core_search:do_delete(SrvId, Set1) of
                {status, {actors_deleted, Num2}} ->
                    do_delete(SrvId, Set2, Num+Num2);
                {error, Error} ->
                    {error, Error}
            end;
        false ->
            case nkdomain_core_search:do_delete(SrvId, ActorIds) of
                {status, {actors_deleted, Num2}} ->
                    {status, {actors_deleted, Num+Num2}};
                {error, Error} ->
                    {error, Error}
            end
    end.


%% ===================================================================
%% Utilities
%% ===================================================================



%% @private
set_debug(SrvId) when is_atom(SrvId) ->
    Debug = nkserver:get_cached_config(SrvId, nkactor, debug) == true,
    put(nkactor_debug, Debug),
    ?REQ_DEBUG("debug started", []).


%% @private
parse_body(#{body:=Body}) when is_map(Body) ->
    nkactor_syntax:parse_actor(Body, #{});

parse_body(#{body:=_}) ->
    {error, request_invalid};

parse_body(_) ->
    {ok, #{}}.


%% @doc Checks that fields in req are not different in body:
check_actor(Actor, ActorId) ->
    #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
    try
        case Actor of
            #{group:=ObjGroup} when ObjGroup /= Group ->
                throw({field_invalid, <<"group">>});
            _ ->
                ok
        end,
        case Actor of
            #{resource:=ObjRes} when ObjRes /= Res ->
                throw({field_invalid, <<"resource">>});
            _ ->
                ok
        end,
        case Actor of
            #{namespace:=ObjNamespace} when ObjNamespace /= Namespace ->
                throw({field_invalid, <<"namespace">>});
            _ ->
                ok
        end,
        case Actor of
            #{name:=ObjName} ->
                % If name is in body, it has been already read, so there is a name
                if
                    is_binary(Name) andalso Name /= ObjName ->
                        throw({field_invalid, <<"name">>});
                    true ->
                        ok
                end;
            _ ->
                ok
        end,
        {ok, Actor#{group=>Group, resource=>Res, name=>Name, namespace=>Namespace}}
    catch
        throw:Throw ->
            {error, Throw}
    end.


%% @private
set_activate_opts(#{activable:=false}, _Req) ->
    #{activate=>false, ot_span_id=>?REQ_SPAN};

set_activate_opts(_Config, Req) ->
    Params = maps:get(params, Req, #{}),
    Activate = maps:get(activate, Params, true),
    Opts1 = case Activate of
        true ->
            #{activate=>true, consume=>maps:get(consume, Params, false)};
        false ->
            #{activate=>false}
    end,
    Opts2 = case maps:find(ttl, Params) of
        {ok, TTL} ->
            Opts1#{ttl=>TTL};
        error ->
            Opts1
    end,
    Opts2#{ot_span_id=>?REQ_SPAN}.


%% @private
get_watches() ->
    nklib_proc:values(core_v1_watches).


%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).