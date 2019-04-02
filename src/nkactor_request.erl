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

-include("nkactor_request.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(DELETE_COLLECTION_SIZE, 10000).


%% ===================================================================
%% Types
%% ===================================================================



%% ===================================================================
%% Public
%% ===================================================================


%% @doc Launches an Request
-spec request(nkactor:request()) ->
    nkactor:response().

request(Req) ->
    % Gets group and vsn from request or body
    case nkactor_syntax:parse_request(Req) of
        {ok, #{namespace:=Namespace}=Req2} ->
            case nkactor_namespace:get_namespace(Namespace) of
                {ok, SrvId, _NamespacePid} ->
                    set_debug(SrvId),
                    ?REQ_DEBUG("incoming request for ~s", [Namespace]),
                    Req3 = Req2#{
                        srv => SrvId,
                        start_time => nklib_date:epoch(usecs),
                        trace => []
                    },
                    Req4 = add_trace(req_start, none, Req3),
                    case ?CALL_SRV(SrvId, actor_authorize, [Req4]) of
                        {true, Req5} ->
                            ?REQ_DEBUG("request is authorized", []),
                            Reply = do_request(Req5),
                            case reply(Reply, Req5) of
                                {raw, {CT, Bin}} ->
                                    ?REQ_DEBUG("reply raw: ~p", [CT], Req5),
                                    {raw, {CT, Bin}, add_trace(api_reply, raw, Req5)};
                                {Status, Data, Req6} ->
                                    ?REQ_DEBUG("reply: ~p", [Reply], Req6),
                                    {Status, Data, add_trace(api_reply, {Status, Data}, Req6)}
                            end;
                        false ->
                            {error, unauthorized, add_trace(api_reply, {error, unauthorized}, Req4)}
                    end;
                {error, Error} ->
                    {error, Error, Req}
            end;
        {error, Error} ->
            {error, Error, Req}
    end.


%% @doc
reply(Term, Req) ->
    case Term of
        Op when Op==ok; Op==created; Op==error; Op==status ->
            {Op, #{}, Req};
        {Op, Reply} when Op==ok; Op==created; Op==error; Op==status ->
            {Op, Reply, Req};
        {raw, {CT, Body}} ->
            {raw, {CT, Body}};
        Other ->
            ?REQ_LOG(error, "Invalid API response: ~p", [Other]),
            {error, internal_error, Req}
    end.


%% @private
-spec do_request(nkactor_api:request()) ->
    nkactor_api:response().

do_request(Req) ->
    try
        #{
            verb := Verb,
            group := Group,
            resource := Res,
            namespace := Namespace
        } = Req,
        ?REQ_DEBUG("incoming '~p' ~s (~p)", [Verb, Res, Req]),
        ActorId = #actor_id{
            group = Group,
            resource = Res,
            name = maps:get(name, Req, <<>>),
            namespace = Namespace
        },
        Config = case catch nkactor_util:get_actor_config(ActorId) of
            {ok, _SrvId, Config0} ->
                Config0;
            {error, ConfigError} ->
                throw({error, ConfigError})
        end,
        #{verbs:=Verbs, module:=Module} = Config,
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
        case nkactor_actor:request(Module, ActorId, Req2) of
            continue when SubRes == [] ->
                ?REQ_DEBUG("processing default", [], Req2),
                default_request(Verb, ActorId, Config, Req2);
            continue ->
                ?REQ_LOG(warning, "Invalid resource.0 (~p)", [{Verb, SubRes, ActorId}]),
                {error, resource_invalid};
            {continue, #{resource:=Res2}=Req3} when Res2 /= Res ->
                ?REQ_DEBUG("updated request", [], Req3),
                do_request(Req2);
            Other ->
                ?REQ_DEBUG("specific processing", [], Req2),
                Other
        end
    catch
        throw:Throw ->
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
    Opts = set_opts(Config, Req),
    ?REQ_DEBUG("processing read ~p (~p)", [ActorId, Opts]),
    case nkactor:get_actor(ActorId, Opts) of
        {ok, Actor} ->
            {ok, Actor};
        {error, ReadError} ->
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
            case check_actor(Actor1, ActorId) of
                {ok, Actor2} ->
                    Opts = set_opts(Config, Req),
                    ?REQ_DEBUG("creating actor ~p ~p", [Actor2, Opts]),
                    case nkactor:create(Actor2, Opts#{get_actor=>true}) of
                        {ok, Actor3} ->
                            {created, Actor3};
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.



%% @doc
update(ActorId, Config, Req) ->
     case parse_body(Req) of
        {ok, Actor1} ->
            case check_actor(Actor1, ActorId) of
                {ok, Actor2} ->
                    Opts = set_opts(Config, Req),
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
add_trace(Op, Meta, Req) ->
    #{trace:=Trace} = Req,
    Now = nklib_date:epoch(usecs),
    Trace2 = case Trace of
        [] ->
            [{undefined, Op, Meta, Now}];
        [{undefined, LastOp, LastMeta, LastTime}|Rest] ->
            [
                {undefined, Op, Meta, Now},
                {LastTime-Now, LastOp, LastMeta, LastTime} |
                Rest
            ]
    end,
    Req#{trace:=Trace2}.


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
set_opts(#{activable:=false}, Req) ->
    Req#{activate=>false, request=>Req};

set_opts(_Config, Req) ->
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
    Opts2#{request => Req}.


%% @private
get_watches() ->
    nklib_proc:values(core_v1_watches).


%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).