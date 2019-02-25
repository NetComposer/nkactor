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

%% @doc Actor Syntax
-module(nkactor_syntax).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([parse_actor/1, parse_actor/2]).
-export([parse_request/1, parse_params/2]).

-include("nkactor.hrl").
-include("nkactor_request.hrl").


%% ===================================================================
%% Syntax
%% ===================================================================

%% @doc
parse_actor(Actor) ->
    parse_actor(Actor, #{}).


%% @doc
parse_actor(Actor, Syntax) ->
    Syntax2 = actor_syntax(Syntax),
    case nklib_syntax:parse(Actor, Syntax2) of
        {ok, Actor2, []} ->
            {ok, Actor2};
        {ok, _, [Field|_]} ->
            {error, {field_unknown, Field}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
actor_syntax(Base) ->
    Base#{
        group => binary,
        resource => binary,
        name => binary,
        namespace => binary,
        uid => binary,
        data => map,
        metadata => meta_syntax(),
        '__defaults' => #{
            data => #{},
            metadata => #{}
        }
    }.


%% @private
meta_syntax() ->
    #{
        vsn => binary,
        hash => binary,
        subtype => binary,
        generation => pos_integer,
        creation_time => date_3339,
        update_time => date_3339,
        is_enabled => boolean,
        is_activated => boolean,
        expires_time => date_3339,
        labels => #{'__key_binary' => binary},
        annotations => #{'__key_binary' => binary},
        links => #{'__key_binary' => binary},
        fts => #{'__key_binary' => binary},
        in_alarm => boolean,
        alarms => {list, binary},
        next_status_time => date_3339,
        description => binary,
        created_by => binary,
        updated_by => binary,
        '__defaults' => #{
            vsn => <<"0">>
        }
    }.


%% @doc
parse_request(Req) ->
    Syntax = request_syntax(),
    case nklib_syntax:parse(Req, Syntax) of
        {ok, #{group:=_, resource:=_, namespace:=_, name:=_}=Req2, _} ->
            {ok, Req2};
        {ok, #{uid:=UID}=Req2, _} ->
            ?REQ_DEBUG("reading UID: ~s", [UID]),
            case nkactor:find(UID) of
                {ok, ActorId} ->
                    ?REQ_DEBUG("UID resolved", []),
                    #actor_id{
                        group=Group,
                        resource=Res,
                        name=Name,
                        namespace=Namespace
                    } = ActorId,
                    Req3 = Req2#{
                        group => Group,
                        resource => Res,
                        name => Name,
                        namespace => Namespace
                    },
                    {ok, Req3};
                {error, Error} ->
                    {error, Error}
            end;
        {ok, #{group:=_, resource:=_, namespace:=_}=Req2, _} ->
            {ok, Req2};
        {ok, #{body:=Body}=Req2, _} when is_map(Body) ->
            case nklib_syntax:parse(Body, body_syntax()) of
                {ok, ParsedBody, _} ->
                    case maps:merge(Req2, ParsedBody) of
                        #{group:=_, resource:=_, namespace:=_}=Req3 ->
                            {ok, Req3};
                        _ ->
                            parse_request_missing(Req2)
                    end;
                _ ->
                    parse_request_missing(Req2)
            end;
        {ok, Req2, _} ->
            parse_request_missing(Req2);
        {error, Error} ->
            {error, Error}
    end.


%% @private
parse_request_missing(Req) ->
    case maps:is_key(group, Req) of
        false ->
            {error, {field_missing, <<"group">>}};
        true ->
            case maps:is_key(resource, Req) of
                false ->
                    {error, {field_missing, <<"resource">>}};
                true ->
                    {error, {field_missing, <<"namespace">>}}
            end
    end.



%% @private
request_syntax() ->
    #{
        verb => atom,
        group => binary,
        namespace => binary,
        resource => binary,
        name => binary,
        uid => binary,
        subresource => fun parse_subresource/1,
        params => map,
        body => [map, binary],
        auth => map,
        external_url => binary,
        srv => atom,
        callback => atom,
        meta => map,
        '__defaults' => #{
            verb => get
        }
    }.


%% @private
body_syntax() ->
    #{
        group => binary,
        namespace => binary,
        resource => binary,
        name => binary
    }.



%% @private
parse_subresource([]) ->
    {ok, []};

parse_subresource([Term|_]=List) when is_binary(Term) ->
    {ok, List};

parse_subresource(Term) when is_binary(Term) ->
    {ok, binary:split(Term, <<"/">>, [global])};

parse_subresource(Term) ->
    parse_subresource(to_bin(Term)).




%% @doc
parse_params(Verb, #{params:=Params}=Req) ->
    Syntax = params_syntax(Verb),
    case nklib_syntax:parse(Params, Syntax, #{allow_unknown=>true}) of
        {ok, Parsed, _} ->
            {ok, Req#{params:=Parsed}};
        {error, {syntax_error, Field}} ->
            {error, {parameter_invalid, Field}};
        {error, Error} ->
            {error, Error}
    end;

parse_params(_Verb, Req) ->
    {ok, Req}.


%% @doc
params_syntax(get) ->
    #{
        activate => boolean,
        consume => boolean,
        ttl => {integer, 1, none}
    };

params_syntax(create) ->
    #{
        activate => boolean,
        ttl => {integer, 1, none}
    };

params_syntax(list) ->
    #{
        from => pos_integer,
        size => pos_integer,
        sort => binary,
        labels => #{'__key_binary' => binary},
        fields => #{'__key_binary' => binary},
        links => #{'__key_binary' => binary},
        fts => binary,
        deep => boolean,
        totals => boolean
    };

params_syntax(delete) ->
    #{
        cascade => boolean
    };


params_syntax(deletecollection) ->
    #{
        from => pos_integer,
        size => pos_integer,
        sort => binary,
        labels => #{'__key_binary' => binary},
        fields => #{'__key_binary' => binary},
        links => #{'__key_binary' => binary},
        fts => binary,
        deep => boolean
    };

params_syntax(watch) ->
    #{
        deep => boolean,
        kind => binary,
        version => binary
    };


params_syntax(_Veb) ->
    #{}.



%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).