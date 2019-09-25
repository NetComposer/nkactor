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


%% @doc Basic Actor utilities
-module(nkactor_util).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-export([fold_actors/7, activate_actors/2]).
-export([pre_create/2, pre_update/4]).

-define(ACTIVATE_SPAN, auto_activate).


%% ===================================================================
%% Public
%% ===================================================================



%% @doc Performs an query on database for actors marked as 'active' and tries
%% to active them if not already activated
-spec fold_actors(nkserver:id(), nkactor:group(), nkactor:resource(), nkactor:namespace(),
                  boolean(), function(), term()) ->
    {ok, [#actor_id{}]} | {error, term()}.

fold_actors(SrvId, Group, Res, Namespace, Deep, FoldFun, FoldAcc) ->
    Search = fun(Start) ->
        #{
            namespace => Namespace,
            deep => Deep,
            size => 100,
            get_data => true,
            get_metadata => true,
            filter => #{
                'and' => lists:flatten([
                    #{field=>uid, op=>gt, value=>Start},
                    #{field=>group, value=>Group},
                    case Res of
                        any ->
                            [];
                        _ ->
                            #{field=>resource, value=>Res}
                    end
                ])
            },
            sort => [#{field=><<"uid">>, order=>asc}]
        }
    end,
    fold_actors(SrvId, <<>>, Search, FoldFun, FoldAcc).


%% @private
fold_actors(SrvId, NextUID, SearchFun, FoldFun, FoldAcc) ->
    Search = SearchFun(NextUID),
    case nkactor:search_actors(SrvId, Search, #{}) of
        {ok, [], _} ->
            FoldAcc;
        {ok, Actors, _} ->
            FoldAcc2 = lists:foldl(
                fun(Actor, Acc) -> FoldFun(Actor, Acc) end,
                FoldAcc,
                Actors),
            [#{uid:=LastUID}|_] = lists:reverse(Actors),
            fold_actors(SrvId, LastUID, SearchFun, FoldFun, FoldAcc2);
        {error, Error} ->
            {error, Error}
    end.



%% @doc Performs an query on database for actors marked as 'active' and tries
%% to active them if not already activated
-spec activate_actors(nkserver:id(), Time::integer()) ->
    {ok, integer()} | {error, term()}.

activate_actors(SrvId, Time) ->
    case nkactor:search_activate(SrvId, Time) of
        {ok, List} ->
            do_activate_actors(List, 0);
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_activate_actors([], Acc) ->
    {ok, Acc};

do_activate_actors([ActorId|Rest], Acc) ->
    Acc2 = case nkactor_namespace:find_registered_actor(ActorId) of
        {true, _, _} ->
            Acc;
        _ ->
            case nkactor:activate(ActorId) of
                {ok, ActorId2} ->
                    nkserver_ot:log(?ACTIVATE_SPAN, {"activated actor ~p", [ActorId2]}),
                    lager:notice("NkACTOR auto-activating ~p", [ActorId2]),
                    Acc+1;
                {error, actor_expired} ->
                    nkserver_ot:log(?ACTIVATE_SPAN, {"activated actor ~p", [ActorId]}),
                    lager:info("NkACTOR expired actor ~p", [ActorId]),
                    Acc+1;
                {error, Error} ->
                    nkserver_ot:log(?ACTIVATE_SPAN, {"could not activated actor ~p: ~p",
                        [ActorId, Error]}),
                    nkserver_ot:tag_error(?ACTIVATE_SPAN, could_not_activate_actor),
                    lager:warning("NkACTOR could not auto-activate ~p: ~p",
                        [ActorId, Error]),
                    Acc
            end
    end,
    do_activate_actors(Rest, Acc2).


%% @private
pre_create(Actor, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    Syntax = #{'__mandatory' => [group, resource, namespace]},
    case nkactor_syntax:parse_actor(Actor, Syntax) of
        {ok, Actor2} ->
            nkserver_ot:log(SpanId, <<"actor parsed">>),
            #{namespace:=Namespace} = Actor2,
            case nkactor_namespace:find_service(Namespace) of
                {ok, SrvId} ->
                    nkserver_ot:log(SpanId, <<"actor namespace found: ~s">>, [SrvId]),
                    case nkactor_lib:add_creation_fields(SrvId, Actor2) of
                        {ok, Actor3} ->
                            Actor4 = case Opts of
                                #{forced_uid:=UID} ->
                                    nomatch = binary:match(UID, <<".">>),
                                    Actor3#{uid := UID};
                                _ ->
                                    Actor3
                            end,
                            Req1 = maps:get(request, Opts, #{}),
                            Req2 = Req1#{srv => SrvId},
                            case nkactor_actor:parse(SrvId, create, Actor4, Req2) of
                                {ok, Actor5} ->
                                    case nkactor_lib:check_actor_links(Actor5) of
                                        {ok, Actor6} ->
                                            nkserver_ot:log(SpanId, <<"actor parsed">>),
                                            {ok, SrvId, Actor6};
                                        {error, Error} ->
                                            nkserver_ot:log(SpanId, <<"error checking links: ~p">>, [Error]),
                                            {error, Error}
                                    end;
                                {error, Error} ->
                                    nkserver_ot:log(SpanId, <<"error parsing specific actor: ~p">>, [Error]),
                                    {error, Error}
                            end;
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error creating initial data: ~p">>, [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    nkserver_ot:log(SpanId, <<"error getting namespace: ~p">>, [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing generic actor: ~p">>, [Error]),
            {error, Error}
    end.



%% @private
pre_update(SrvId, ActorId, Actor, Opts) ->
    SpanId = maps:get(ot_span_id, Opts, undefined),
    case nkactor_syntax:parse_actor(Actor, #{}) of
        {ok, Actor2} ->
            #actor_id{group=Group, resource=Res, name=Name, namespace=Namespace} = ActorId,
            Base = #{group=>Group, resource=>Res, name=>Name, namespace=>Namespace},
            Actor3 = maps:merge(Base, Actor2),
            nkserver_ot:log(SpanId, <<"actor parsed">>),
            Req1 = maps:get(request, Opts, #{}),
            Req2 = Req1#{srv => SrvId},
            case nkactor_actor:parse(SrvId, update, Actor3, Req2) of
                {ok, Actor4} ->
                    case nkactor_lib:check_actor_links(Actor4) of
                        {ok, Actor5} ->
                            nkserver_ot:log(SpanId, <<"actor parsed">>),
                            {ok, Actor5};
                        {error, Error} ->
                            nkserver_ot:log(SpanId, <<"error checking links: ~p">>, [Error]),
                            {error, Error}
                    end;
                {error, Error} ->
                    nkserver_ot:log(SpanId, <<"error parsing specific actor: ~p">>, [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            nkserver_ot:log(SpanId, <<"error parsing generic actor: ~p">>, [Error]),
            {error, Error}
    end.



