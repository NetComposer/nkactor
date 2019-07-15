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
-export([parse_actor/1, parse_actor/2, parse_request/1]).
-export([actor_fields_filter/0, actor_fields_sort/0, actor_fields_trans/0,
         actor_fields_type/0, actor_fields_static/0]).
-include("nkactor.hrl").
-include("nkactor_request.hrl").


%% ===================================================================
%% Syntax
%% ===================================================================

%% @doc
parse_actor(Actor) ->
    Syntax = #{
        '__mandatory' => [group, resource, name, namespace, uid]
    },
    parse_actor(Actor, Syntax).


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
        vsn => binary,
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
%% snake_case and camel_case are allowed, but converted to snake_case always
meta_syntax() ->
    #{
        kind => binary,
        vsn => binary,
        hash => binary,
        subtype => binary,
        generation => pos_integer,
        creation_time => date_3339,
        creationTime => {'__key', creation_time, date_3339},
        update_time => date_3339,
        updateTime => {'__key', update_time, date_3339},
        is_enabled => boolean,
        isEnabled => {'__key', is_enabled, boolean},
        is_active => boolean,
        isActive => {'__key', is_active, boolean},
        expires_time => [date_3339, {binary, [<<>>]}],
        expiresTime=> {'__key', expires_time, [date_3339, {binary, [<<>>]}]},
        labels => #{'__key_binary' => binary},
        annotations => #{'__key_binary' => binary},
        links => #{'__key_binary' => binary},
        fts => #{'__key_binary' => binary},
        in_alarm => boolean,
        inAlaram => {'__key', in_alarm, boolean},
        alarms => {list, binary},
        next_status_time => date_3339,
        nextStatusTime => {'__key', next_status_time, date_3339},
        description => binary,
        created_by => binary,
        updated_by => binary,
        trace_id => integer
    }.


%% @doc
%% - If request has group, resource, namespace and name, ok
%% - If not, but have uid, find it and fill all data
%% - If not, but has group, resource and namespace, ok (no name)
%% - If not, find id from body and merge
parse_request(Req) ->
    Syntax = request_syntax(),
    case nklib_syntax:parse(Req, Syntax) of
        {ok, #{uid:=UID}=Req2, _} ->
            ?REQ_DEBUG("reading UID: ~s", [UID]),
            case nkactor:find(UID) of
                {ok, ActorId} ->
                    ?REQ_DEBUG("UID resolved", []),
                    #actor_id{
                        group = Group,
                        resource = Res,
                        name = Name,
                        namespace = Namespace
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
        {ok, Req2, _} ->
            parse_request_body(Req2);
        {error, Error} ->
            {error, Error}
    end.


%% @private
parse_request_body(#{verb:=Verb, body:=Body}=Req) when Verb==create; Verb==update ->
    case nklib_syntax:parse(Body, body_syntax()) of
        {ok, ParsedBody, _} ->
            case {ParsedBody, Req} of
                {#{group:=G1}, #{group:=G2}} when G1 /= G2 ->
                    {error, {field_invalid, <<"group">>}};
                {#{resource:=R1}, #{resource:=R2}} when R1 /= R2 ->
                    {error, {field_invalid, <<"resource">>}};
                {#{name:=N1}, #{name:=N2}} when N1 /= N2 ->
                    {error, {field_invalid, <<"name">>}};
                {#{namespace:=S1}, #{namespace:=S2}} when S1 /= S2 ->
                    {error, {field_invalid, <<"namespace">>}};
                _ ->
                    case maps:merge(Req, ParsedBody) of
                        #{group:=_, resource:=_}=Req2 ->
                            {ok, Req2};
                        _ ->
                            parse_request_missing(Req)
                    end
            end;
        {error, Error} ->
            {error, Error}
    end;

parse_request_body(#{group:=_, resource:=_}=Req) ->
    {ok, Req};

parse_request_body(Req) ->
    parse_request_missing(Req).


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
        srv => atom,
        class => any,
        verb => atom,
        namespace => binary,
        group => binary,
        vsn => binary,
        resource => binary,
        name => binary,
        uid => binary,
        subresource => binary,
        params => map,
        content_type => binary,
        body => [map, binary],
        auth => map,
        ot_span_id => any,
        external_url => binary,
        callback => atom,
        meta => map,
%%        '__defaults' => #{
%%            verb => get
%%        },
        '__mandatory' => [srv]
    }.


%% @private
body_syntax() ->
    #{
        group => binary,
        namespace => binary,
        resource => binary,
        name => binary
    }.



%% ===================================================================
%% Common fields
%% ===================================================================

%% @private Called from nkactor_callbacks
actor_fields_filter() ->
    [
        uid,
        group,
        resource,
        'group+resource',           % Maps to special group + resource
        name,
        namespace,
        'metadata.kind',
        'metadata.subtype',
        'metadata.vsn',
        'metadata.hash',
        'metadata.generation',
        'metadata.creation_time',
        'metadata.update_time',
        'metadata.is_active',
        'metadata.expires_time',
        'metadata.is_enabled',
        'metadata.in_alarm',
        'metadata.next_status_time'
    ].


%% @private Called from nkactor_callbacks
actor_fields_sort() ->
    [
        group,
        resource,
        'group+resource',
        name,
        namespace,
        path,
        'metadata.kind',
        'metadata.subtype',
        'metadata.vsn',
        'metadata.generation',
        'metadata.creation_time',
        'metadata.update_time',
        'metadata.is_active',
        'metadata.expires_time',
        'metadata.is_enabled',
        'metadata.in_alarm',
        'metadata.next_status_time'
    ].



%% @private Called from nkactor_callbacks
actor_fields_trans() ->
    #{
        kind => 'metadata.kind',
        'metadata.creationTime' => 'metadata.creation_time',
        'metadata.updateTime' => 'metadata.update_time',
        'metadata.isActive' => 'metadata.is_active',
        'metadata.expiresTime'=> 'metadata.expires_time',
        'metadata.isEnabled' => 'metadata.is_enabled',
        'metadata.inAlaram' => 'metadata.in_alarm',
        'metadata.nextStatusTime' => 'metadata.next_status_time'
    }.


%% @private Called from nkactor_callbacks
actor_fields_type() ->
    #{
        'metadata.generation' => integer,
        'metadata.is_active' => boolean,
        'metadata.is_enabled' => boolean,
        'metadata.in_alarm' => boolean
    }.



%% @private Called from nkactor_callbacks
actor_fields_static() ->
    [].


%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).