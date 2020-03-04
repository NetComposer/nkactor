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
-import(nkserver_trace, [trace/1, trace/2, log/3]).
-define(ACTIVATE_SPAN, auto_activate).


%% ===================================================================
%% Public
%% ===================================================================


-type fold_fun() :: fun((nkactor:actor(), Acc::term()) -> Acc::term() | {stop, Reason::term()}).

%% @doc Performs an query on database for actors marked as 'active' and tries
%% to active them if not already activated
-spec fold_actors(nkserver:id(), nkactor:group(), nkactor:resource(), nkactor:namespace(),
                  boolean(), fold_fun(), term()) ->
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
            case do_fold_actors(Actors, FoldFun, FoldAcc) of
                {stop, Reason} ->
                    {stop, Reason};
                FoldAcc2 ->
                    [#{uid:=LastUID}|_] = lists:reverse(Actors),
                    fold_actors(SrvId, LastUID, SearchFun, FoldFun, FoldAcc2)
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_fold_actors([], _FoldFun, FoldAcc) ->
    FoldAcc;

do_fold_actors([Actor|Rest], FoldFun, FoldAcc) ->
    case FoldFun(Actor, FoldAcc) of
        {stop, Reason} ->
            {stop, Reason};
        FoldAcc2 ->
            do_fold_actors(Rest, FoldFun, FoldAcc2)
    end.



%% @doc Performs an query on database for actors marked as 'active' and tries
%% to active them if not already activated
%% Look for actors to be activated from 0 to up to now + Time (msecs)

-spec activate_actors(nkserver:id(), TimeMsecs::integer()) ->
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
                    log(info, "activated actor ~p", [ActorId2]),
                    Acc+1;
                {error, actor_expired} ->
                    log(info, "activated actor ~p", [ActorId]),
                    lager:info("NkACTOR expired actor ~p", [ActorId]),
                    Acc+1;
                {error, Error} ->
                    nkserver_trace:log(notice, "could not activated actor ~p: ~p", [ActorId, Error]),
                    nkserver_trace:error(could_not_activate_actor),
                    lager:warning("NkACTOR could not auto-activate ~p: ~p",
                        [ActorId, Error]),
                    Acc
            end
    end,
    do_activate_actors(Rest, Acc2).






%% ===================================================================
%% LOAD utility
%% ===================================================================

%%%% @doc
%%load_actors_path(Path, Token) ->
%%    Files = filelib:fold_files(Path, "\\.yaml$", false, fun(F, Acc) -> [F|Acc] end, []),
%%    load_actor_files(Files, Token).
%%
%%
%%load_actor_data(Data, Token) ->
%%    case catch nklib_yaml:decode(Data) of
%%        {error, {yaml_decode_error, Error}} ->
%%            {error, {yaml_decode_error, Error}};
%%        Objs ->
%%            load_actor_objs(Objs, Token, [])
%%    end.
%%
%%
%%%% @private
%%load_actor_files([], _Token) ->
%%    ok;
%%
%%load_actor_files([File|Rest], Token) ->
%%    {ok, Data} = file:read_file(File),
%%    case catch nklib_yaml:decode(Data) of
%%        {error, {yaml_decode_error, Error}} ->
%%            lager:warning("Error processing file '~s': ~p", [File, Error]),
%%            {error, {yaml_decode_error, Error}};
%%        Objs ->
%%            lager:warning("Loading actors in file '~s'", [File]),
%%            case load_actor_objs(Objs, Token, []) of
%%                ok ->
%%                    load_actor_files(Rest, Token);
%%                {error, Error} ->
%%                    {error, Error}
%%            end
%%    end.
%%
%%
%%%% @private
%%load_actor_objs([], _Token, Acc) ->
%%    {ok, lists:reverse(Acc)};
%%
%%load_actor_objs([Obj|Rest], Token, Acc) ->
%%    case Obj of
%%        #{<<"metadata">> := #{<<"name">>:=Name}} ->
%%            Op = #{
%%                verb => update,
%%                body => Obj,
%%                auth => #{token => Token}
%%            },
%%            Res = case nkdomain_api:request(?ROOT_SRV, Op) of
%%                {error, Error, _} ->
%%                    {error, Error};
%%                {created, Obj2, _} ->
%%                    #{<<"metadata">>:=#{<<"uid">>:=UID, <<"selfLink">>:=Self}}= Obj2,
%%                    lager:warning("Actor ~s (~s) create", [Self, UID]),
%%                    created;
%%                {ok, Obj2, _} ->
%%                    #{<<"metadata">>:=#{<<"uid">>:=UID, <<"selfLink">>:=Self}}= Obj2,
%%                    lager:warning("Actor ~s (~s) update", [Self, UID]),
%%                    updated
%%            end,
%%            load_actor_objs(Rest, Token, [{Name, Res}|Acc]);
%%        _ ->
%%            % We don't want automatic generation of names in bulk loading
%%            {error, {field_missing, <<"metadata.name">>}}
%%    end.














