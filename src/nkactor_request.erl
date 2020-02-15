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

-export([request/1, pre_request/1, do_request/1, post_request/2]).
-export([parse_params/2]).
-import(nkserver_trace, [trace/1, trace/2, event/1, event/2, log/2, log/3]).
-export_type([request/0, response/0, reply/0]).

-include("nkactor_request.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-define(DELETE_COLLECTION_SIZE, 10000).


%% vsn management
%% --------------
%%
%% - any request can come with field 'vsn'
%% - if an actor is included in body with 'metadata.vsn', it must be the same as request
%% - if vsn is included, it must be one of the supported actor versions
%% - if special request is used, the actor callback must check the vsn
%% - if standard request is used:
%%      - for 'get', 'list', the actor is returned as is (with stored version)
%%      - for 'delete', 'deletecollection', it is not used
%%      - for 'create', 'update', the callback parse/3 must check the version, and
%%        return the parsed version, that is stored with actor
%%

%% ===================================================================
%% Types
%% ===================================================================

-type request() ::
    #{
        verb => nkactor:verb(),
        namespace => nkactor:namespace(),
        group => nkactor:group(),
        % Version of the API request (not used currently by core)
        % It can be used to indicate a specific version of the API should be used
        % If the body has a metadata.vsn field, it must be the same
        vsn => nkactor:vsn(),
        resource => nkactor:resource(),
        name => nkactor:name(),
        % If uid is used, found actor must match namespace, group, resource, name
        uid => nkactor:uid(),
        subresource => nkactor:subresource(),
        params => #{binary() => binary()},
        % Class of the request, can be used for specific processing, not used here
        class => class(),
        % Used to decode the body
        content_type => binary(),
        % If the body has a group, resource, namespace, uid, or metadata
        % fields, they must be the same of the request
        body => binary() | map(),
        auth => map(),
        % Implementing nkdomain_api behaviour:
        callback => module(),
        % External url to use in callbacks:
        external_url => binary(),
        % Supported keys in meta:
        % - search_field_trans
        meta => map(),
        %% added by system:
        srv => nkserver:id(),            % Service managing namespace
        start_time => nklib_date:epoch(usecs)
    }.


-type response() ::
    ok | {ok, map()} | {ok, map(), request()} |
    created | {created, map()} | {created, map(), request()} |
    {status, nkserver:status()} |
    {status, nkserver:status(), map()} |  %% See status/2
    {status, nkserver:status(), map(), request()} |
    {error, nkserver:status()} |
    {error, nkserver:status(), request()}.


-type reply() ::
    {ok|created, map(), request()} |
    {raw, {CT::binary(), Body::binary()}, request()} |
    {status|error, nkserver:status(), request()}.

-type class() :: term().


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Launches an Request
%% - Calls actor_req_authorize/1 for authorization of the request
%% - Finds service managing namespace, and adds srv and start_time

-spec request(request()) ->
    reply().

request(Req) ->
    case pre_request(Req) of
        {ok, Req2} ->
            #{
                group := Group,
                resource := Res,
                namespace := Ns,
                verb := Verb,
                subresource := SubRes,
                srv := SrvId
            } = Req2,
            Fun = fun() ->
                Tags = #{
                    <<"req.verb">> => Verb,
                    <<"req.group">> => Group,
                    <<"req.resource">> => Res,
                    <<"req.namespace">> => Ns,
                    <<"req.name">> => maps:get(name, Req, <<>>),
                    <<"req.srv">> => SrvId,
                    <<"req.subresource">> => SubRes
                },
                nkserver_trace:tags(Tags),
                set_debug(SrvId),
                case authorize(SrvId, Req2) of
                    {true, Req3} ->
                        Reply = do_request(Req3),
                        post_request(Reply, Req3);
                    false ->
                        {error, unauthorized, Req2}
                end
            end,
            SpanOpts = #{
                metadata => #{
                    app => SrvId,
                    group => actor_request,
                    resource => Verb,
                    namespace => Ns,
                    verb => Verb,
                    subresource => SubRes
                }
            },
            nkserver_trace:new_span(SrvId, nkactor_request, Fun, SpanOpts);
        {error, Error, Req2} ->
            {error, Error, Req2}
    end.


%% @private
pre_request(Req) ->
    trace("parsing request"),
    case nkactor_syntax:parse_request(Req) of
        {ok, #{namespace:=Ns}=Req2} ->
            case nkactor_namespace:find_service(Ns) of
                {ok, SrvId} ->
                    trace("request service is ~s (~s)", [SrvId, Ns]),
                    Req3 = Req2#{
                        srv => SrvId,
                        verb => maps:get(verb, Req2, get),
                        subresource => maps:get(subresource, Req2, <<>>),
                        start_time => nklib_date:epoch(usecs)
                    },
                    {ok, Req3};
                {error, Error} ->
                    {error, Error, Req2}
            end;
        {error, Error} ->
            {error, Error, Req}
    end.


%% @doc
post_request(Reply, Req) ->
    case reply(Reply, Req) of
        {raw, {CT, Bin}, Req2} ->
            trace("request reply 'raw': ~s", [CT]),
            {raw, {CT, Bin}, Req2};
        {Status, Data, Req2} ->
            trace("request reply '~s'", [Status]),
            {Status, Data, Req2}
    end.


%% @doc
reply(Op, Req) when Op==ok; Op==created; Op==error; Op==status ->
    {Op, #{}, Req};

reply({Op, Reply}, Req) when Op==ok; Op==created; Op==error; Op==status ->
    {Op, Reply, Req};

reply({Op, Reply, Req2}, _Req) when Op==ok; Op==created; Op==error; Op==status ->
    {Op, Reply, Req2};

reply({raw, {CT, Bin}}, Req) ->
    {raw, {CT, Bin}, Req};

reply(Other, Req) ->
    ?REQ_LOG(error, "Invalid API response: ~p", [Other]),
    {error, internal_error, Req}.


%% @private
-spec do_request(request()) ->
    response().

do_request(Req) ->
    try
        #{
            srv := SrvId,
            verb := Verb,
            group := Group,
            resource := Res,
            subresource := SubRes,
            namespace := Namespace
        } = Req,
        ?REQ_DEBUG("incoming '~p' ~s (~p)", [Verb, Res, Req]),
        trace("starting request processing"),
        ActorId = #actor_id{
            group = Group,
            resource = Res,
            name = maps:get(name, Req, undefined),
            namespace = Namespace
        },
        Config = case catch nkactor_actor:get_config(SrvId, ActorId) of
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
        trace("calling specific processing"),
        case nkactor_actor:request(SrvId, ActorId, Req) of
            continue when SubRes == <<>> ->
                trace("processing default"),
                ?REQ_DEBUG("processing default", [], Req),
                default_request(Verb, ActorId, Config, Req);
            continue ->
                log(notice, "invalid subresource (~s:~s) ~w", [Verb, SubRes, ActorId]),
                nkserver_trace:error(resource_invalid),
                {error, resource_invalid};
            {continue, #{resource:=Res2}=Req3} when Res2 /= Res ->
                trace("request updated. New resource: ~p", [Res2]),
                do_request(Req3);
            Other ->
                trace("processed specific"),
                Other
        end
    catch
        throw:Throw ->
            trace("request processing error: ~p", [Throw]),
            nkserver_trace:error(Throw),
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
    check_version(Config, Req),
    create(ActorId, Config, Req);

default_request(update, #actor_id{name=Name}=ActorId, Config, Req) when is_binary(Name) ->
    check_version(Config, Req),
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
    ParamsSyntax = #{
        activate => boolean,
        consume => boolean,
        ttl => {integer, 1, none}
    },
    case parse_params(Req, ParamsSyntax) of
        {ok, Params} ->
            Opts = set_activate_opts(Config, Params),
            trace("processing standard read (~p)", [Opts]),
            % request is included in case it could be used for specific parsing
            % when calling nkactor_actor:parse()
            case nkactor:get_actor(ActorId, Opts#{request=>Req}) of
                {ok, Actor} ->
                    {ok, Actor};
                {error, actor_not_found} ->
                    {error, actor_not_found};
                {error, ReadError} ->
                    log(notice, "error getting actor: ~p", [ReadError]),
                    nkserver_trace:error(ReadError),
                    {error, ReadError}
            end;
        {error, Error} ->
            log(notice, "error parsing params: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.


%% @doc
list(ActorId, Config, Req) ->
    {SrvId, Base, Opts1} = get_search_spec(ActorId, Config, Req),
    Opts2 = Opts1#{
        default_spec => #{
            get_data => true,
            get_metadata => true,
            sort => [#{field=><<"metadata.update_time">>, order=>desc}]
        }
    },
    %io:format("NKLOG SPEC ~s\n", [nklib_json:encode_pretty(Opts2)]),
    trace("processing standard read: ~p, ~p", [Base, Opts2]),
    case nkactor:search_actors(SrvId, Base, Opts2) of
        {ok, ActorList, Meta} ->
            {ok, Meta#{items=>ActorList}};
        {error, Error} ->
            log(notice, "error calling search: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.


%% @doc
create(ActorId, Config, Req) ->
    ParamsSyntax = #{
        activate => boolean,
        forced_uid => binary,
        ttl => {integer, 1, none}
    },
    case parse_params(Req, ParamsSyntax) of
        {ok, Params} ->
            trace("calling parse body"),
            case parse_body(Req) of
                {ok, Actor1} ->
                    trace("calling check actor"),
                    case check_actor(Actor1, ActorId, Req) of
                        {ok, Actor2} ->
                            Opts = set_activate_opts(Config, Params),
                            event(actor_create, #{opts=>Opts}),
                            % request is included in case it could be used for specific parsing
                            % when calling nkactor_actor:parse()
                            CreateOpts = Opts#{get_actor=>true, request=>Req},
                            case nkactor:create(Actor2, CreateOpts) of
                                {ok, Actor3} ->
                                    {created, Actor3};
                                {error, actor_already_exists} ->
                                    {error, actor_already_exists};
                                {error, Error} ->
                                    log(notice, "error creating actor: ~p", [Error]),
                                    nkserver_trace:error(Error),
                                    {error, Error}
                            end;
                        {error, Error} ->
                            log(notice, "error checking actor: ~p", [Error]),
                            nkserver_trace:error(Error),
                            {error, Error}
                    end;
                {error, Error} ->
                    log(notice, "error parsing body: ~p", [Error]),
                    nkserver_trace:error(Error),
                    {error, Error}
            end;
        {error, Error} ->
            log(notice, "error parsing params: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.


%% @doc
update(ActorId, Config, Req) ->
    ParamsSyntax = #{
        allow_name_change => boolean
    },
    case parse_params(Req, ParamsSyntax) of
        {ok, Params} ->
            trace("calling parse body"),
             case parse_body(Req) of
                {ok, Actor1} ->
                    trace("calling check actor"),
                    case check_actor(Actor1, ActorId, Req) of
                        {ok, Actor2} ->
                            Opts1 = set_activate_opts(Config, Params),
                            Opts2 = Opts1#{get_actor=>true},
                            trace("processing standard update ~p, (~p)", [Actor2, Opts2]),
                            case nkactor:update(ActorId, Actor2, Opts2#{request=>Req}) of
                                {ok, Actor3} ->
                                    {ok, Actor3};
                                {error, actor_not_found} ->
                                    default_request(create, ActorId, Config, Req);
                                {error, {field_missing, name}} ->
                                    default_request(create, ActorId, Config, Req);
                                {error, UpdateError} ->
                                    trace("error updating actor: ~p", [UpdateError]),
                                    nkserver_trace:error(UpdateError),
                                    {error, UpdateError}
                            end;
                        {error, Error} ->
                            log(notice, "error checking actor: ~p", [Error]),
                            nkserver_trace:error(Error),
                            {error, Error}
                    end;
                 {error, Error} ->
                     log(notice, "error parsing body: ~p", [Error]),
                     nkserver_trace:error(Error),
                     {error, Error}
             end;
        {error, Error} ->
            log(notice, "error parsing params: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.


%% @doc
delete(ActorId, _Config, Req) ->
    ?REQ_DEBUG("processing delete ~p", [ActorId]),
    ParamsSyntax = #{
    },
    case parse_params(Req, ParamsSyntax) of
        {ok, _Params} ->
            trace("processing standard delete: ~p", [ActorId]),
            case nkactor:delete(ActorId, #{request=>Req}) of
                ok ->
                    {status, actor_deleted};
                {error, actor_not_found} ->
                    {error, actor_not_found};
                {error, Error} ->
                    log(notice, "error deleting actor: ~p", [Error]),
                    nkserver_trace:error(Error),
                    {error, Error}
            end;
        {error, Error} ->
            log(notice, "error parsing params: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.


delete_collection(ActorId, Config, Req) ->
    {SrvId, Base, Opts1} = get_search_spec(ActorId, Config, Req),
    #{forced_spec:=Forced} = Opts1,
    Opts2 = Opts1#{
        forced_spec => Forced#{only_uid => true},
        default_spec => #{
            sort => [#{field=><<"metadata.update_time">>, order=>asc}]
        }
    },
    trace("processing standard delete_multi: ~p ~p", [Base, Opts2]),
    case nkactor:delete_multi(SrvId, Base, Opts2) of
        {ok, Meta} ->
            {ok, Meta};
        {error, Error} ->
            log(notice, "error processing delete_multi: ~p", [Error]),
            nkserver_trace:error(Error),
            {error, Error}
    end.



%%params_syntax(watch) ->
%%    #{
%%        deep => boolean,
%%        kind => binary,
%%        version => binary
%%    };


%% ===================================================================
%% Utilities
%% ===================================================================



%% @private
set_debug(SrvId) when is_atom(SrvId) ->
    Debug = nkserver:get_cached_config(SrvId, nkactor, debug) == true,
    put(nkactor_debug, Debug),
    ?REQ_DEBUG("debug started", []).


%% @private
check_version(Config, Req) ->
    case maps:get(vsn, Req, <<>>) of
        <<>> ->
            ok;
        Vsn ->
            #{versions:=Versions} = Config,
            case lists:member(Vsn, Versions) of
                true ->
                    ok;
                false ->
                    throw({error, vsn_not_allowed})
            end
    end.


%% @private
parse_body(#{body:=Actor}) when is_map(Actor) ->
    nkactor_syntax:parse_actor(Actor, #{});

parse_body(#{body:=_}) ->
    {error, request_invalid};

parse_body(_) ->
    {ok, #{}}.


%% @doc Checks that fields in req are not different in body:
check_actor(Actor, ActorId, Req) ->
    #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
    AllowChange = case Req of
        #{verb:=update, params:=#{allow_name_change:=true}} ->
            true;
        _ ->
            false
    end,
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
        UpdNamespace = case Actor of
            #{namespace:=ObjNamespace} ->
                if
                    ObjNamespace /= Namespace, not AllowChange ->
                        throw({field_invalid, <<"namespace3">>});
                    true ->
                        ObjNamespace
                end;
            _ ->
                Namespace
        end,
        UpdName = case Actor of
            #{name:=ObjName} ->
                % If name is in body, it has been already read, so there is a name
                if
                    is_binary(Name) andalso Name /= ObjName, not AllowChange ->
                        throw({field_invalid, <<"name">>});
                    true ->
                        ObjName
                end;
            _ ->
                Name
        end,
        case Actor of
            #{metadata:=#{vsn:=ObjVsn}} ->
                case maps:get(vsn, Req, ObjVsn) of
                    ObjVsn ->
                        ok;
                    _ ->
                        throw({field_invalid, <<"metadata.vsn">>})
                end;
            _ ->
                ok
        end,
        {ok, Actor#{group=>Group, resource=>Res, name=>UpdName, namespace=>UpdNamespace}}
    catch
        throw:Throw ->
            {error, Throw}
    end.


%% @private
set_activate_opts(#{activable:=false}, Params) ->
    Params#{activate=>false};

set_activate_opts(_Config, Params) ->
    Params.


%% @private
authorize(SrvId, Req) ->
    trace("calling authorize"),
    case ?CALL_SRV(SrvId, actor_req_authorize, [Req]) of
        true ->
            event(actor_request_authorized),
            {true, Req};
        {true, Req2} ->
            event(actor_request_authorized),
            {true, Req2};
        false ->
            event(actor_request_not_authorized),
            false
    end.


%% @private
get_search_spec(ActorId, Config, Req) ->
    #actor_id{namespace=Namespace, group=Group, resource=Res} = ActorId,
    #{srv:=SrvId} = Req,
    Base = case maps:get(body, Req, #{}) of
        <<>> ->
            #{};
        Body ->
            Body
    end,
    Opts1 = maps:with([fields_filter, fields_sort, fields_type], Config),
    Params = maps:get(params, Req, #{}),
    Opts2 = Opts1#{
        forced_spec => #{
            namespace => Namespace,
            filter => #{
                'and' => [
                    #{field=>group, value=>Group},
                    #{field=>resource, value=>Res}
                ]
            }
        },
        params => maps:remove(fields_trans, Params)
    },
    Opts3 = case Params of
        #{fields_trans:=Trans} ->
            Opts2#{fields_trans => Trans};
        _ ->
            Opts2
    end,
    {SrvId, Base, Opts3}.


%% @private
parse_params(#{params:=Params}, Syntax) ->
    case nklib_syntax:parse_all(Params, Syntax) of
        {ok, Parsed} ->
            {ok, Parsed};
        {error, {syntax_error, Field}} ->
            {error, {parameter_invalid, Field}};
        {error, Error} ->
            {error, Error}
    end;

parse_params(_Req, _Syntax) ->
    {ok, #{}}.




%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).
