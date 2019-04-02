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

%% @doc Default plugin callbacks
-module(nkactor_store_pgsql_callbacks).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').


-export([actor_db_init/1,
         actor_db_find/3, actor_db_read/3, actor_db_create/3, actor_db_update/3,
         actor_db_delete/3, actor_db_search/3, actor_db_aggregate/3]).


%% ===================================================================
%% Persistence callbacks
%% ===================================================================

-type id() :: nkserver:id().
-type actor_id() :: nkactor:actor_id().
-type actor() :: nkactor:actor().

-type continue() :: nkserver_callbacks:continue().

-type opts() :: #{


    cascade => boolean(),
    span_local_id => nkserver_ot:id()
}.


%% @doc Called after the core has initialized the database
-spec actor_db_init(nkserver:id()) ->
    ok | {error, term()} | continue().

actor_db_init(_SrvId) ->
    ok.


%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(id(), actor_id(), opts()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_find(SrvId, ActorId, Opts) ->
    call(SrvId, find, [ActorId], Opts).


%% @doc Must find and read a full actor on disk by UID (if available) or name
-spec actor_db_read(id(), actor_id(), opts()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()} | continue().

actor_db_read(SrvId, ActorId, Opts) ->
    call(SrvId, read, [ActorId], Opts).


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(id(), actor(), opts()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()} | continue().

actor_db_create(SrvId, Actor, Opts) ->
    call(SrvId, create, [Actor], Opts).


%% @doc Must update a new actor on disk.
-spec actor_db_update(id(), actor(), opts()) ->
    {ok, Meta::map()} | {error, term()} | continue().

actor_db_update(SrvId, Actor, Opts) ->
    call(SrvId, update, [Actor], Opts).


%% @doc
-spec actor_db_delete(id(), [nkactor:uid()], opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_delete(SrvId, UIDs, Opts) ->
    call(SrvId, delete, [UIDs, Opts], Opts).


%% @doc
-spec actor_db_search(id(), nkactor_backend:search_type(), opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_search(SrvId, Type, Opts) ->
    PgSrvId = nkactor_store_pgsql:get_pgsql_srv(SrvId),
    start_span(PgSrvId, <<"search">>, Opts),
    Result = case nkactor_store_pgsql_search:search(Type, Opts) of
        {query, Query, Fun} ->
            nkactor_store_pgsql:query(PgSrvId, Query, #{result_fun=>Fun, nkactor_params=>Opts});
        {error, Error} ->
            {error, Error}
    end,
    stop_span(),
    Result.


%% @doc
-spec actor_db_aggregate(id(), nkactor_backend:agg_type(), opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()} | continue().

actor_db_aggregate(SrvId, Type, Opts) ->
    PgSrvId = nkactor_store_pgsql:get_pgsql_srv(SrvId),
    start_span(PgSrvId, <<"aggregate">>, Opts),
    Result = case nkactor_store_pgsql_aggregation:aggregation(Type, Opts) of
        {query, Query, Fun} ->
            nkactor_store_pgsql:query(PgSrvId, Query, #{result_fun=>Fun});
        {error, Error} ->
            {error, Error}
    end,
    stop_span(),
    Result.



%% ===================================================================
%% Internal
%% ===================================================================

%% @private
call(SrvId, Op, Args, Opts) ->
    PgSrvId = nkactor_store_pgsql:get_pgsql_srv(SrvId),
    start_span(PgSrvId, Op, Opts),
    Result = apply(nkactor_store_pgsql_actors, Op, [PgSrvId|Args]),
    stop_span(),
    Result.


%% @private
start_span(SrvId, Op, Opts) ->
    case Opts of
        #{parent_span:=Parent} ->
            SpanName = <<"PGSQL::", (nklib_util:to_binary(Op))/binary>>,
            nkserver_ot:new(actor_store_pgsql, SrvId, SpanName, Parent);
        #{trace_id:=TraceId} ->
            SpanName = <<"PGSQL::", (nklib_util:to_binary(Op))/binary>>,
            nkserver_ot:new(actor_store_pgsql, SrvId, SpanName, {TraceId, undefined});
        _ ->
            ok
    end.


%% @private
stop_span() ->
    nkserver_ot:finish(actor_store_pgsql).
