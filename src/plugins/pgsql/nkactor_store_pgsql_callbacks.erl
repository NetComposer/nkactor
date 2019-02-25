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


-export([actor_db_find/2, actor_db_read/2, actor_db_create/2, actor_db_update/2,
    actor_db_delete/3, actor_db_search/3, actor_db_aggregate/3]).



%% ===================================================================
%% Persistence callbacks
%% ===================================================================

-type id() :: nkserver:id().
-type actor_id() :: nkactor:actor_id().
-type actor() :: nkactor:actor().


%% @doc Must find an actor on disk by UID (if available) or name, and return
%% full actor_id data
-spec actor_db_find(id(), actor_id()) ->
    {ok, actor_id(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_find(SrvId, ActorId) ->
    nkactor_store_pgsql_actors:find(SrvId, ActorId).


%% @doc Must find and read a full actor on disk by UID (if available) or name
-spec actor_db_read(id(), actor_id()) ->
    {ok, nkactor:actor(), Meta::map()} | {error, actor_not_found|term()}.

actor_db_read(SrvId, ActorId) ->
    nkactor_store_pgsql_actors:read(SrvId, ActorId).


%% @doc Must create a new actor on disk. Should fail if already present
-spec actor_db_create(id(), actor()) ->
    {ok, Meta::map()} | {error, uniqueness_violation|term()}.

actor_db_create(SrvId, Actor) ->
    nkactor_store_pgsql_actors:save(SrvId, create, Actor).


%% @doc Must update a new actor on disk.
-spec actor_db_update(id(), actor()) ->
    {ok, Meta::map()} | {error, term()}.


actor_db_update(SrvId, Actor) ->
    nkactor_store_pgsql_actors:save(SrvId, update, Actor).


%% @doc
-spec actor_db_delete(id(), [nkactor:uid()], #{cascade=>boolean()}) ->
    {ok, [actor_id()], Meta::map()} | {error, term()}.


actor_db_delete(SrvId, UIDs, Opts) ->
    nkactor_store_pgsql_actors:delete(SrvId, UIDs, Opts).


%% @doc
-spec actor_db_search(id(), nkactor_backend:search_type(), map()) ->
    term().

actor_db_search(SrvId, Type, Params) ->
    case nkactor_store_pgsql_search:search(Type, Params) of
        {query, Query, Fun} ->
            nkactor_store_pgsql:query(SrvId, Query, #{result_fun=>Fun, nkactor_params=>Params});
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec actor_db_aggregate(id(), nkactor_backend:agg_type(), nkactor_backend:opts()) ->
    term().

actor_db_aggregate(SrvId, Type, Opts) ->
    case nkactor_store_pgsql_aggregation:aggregation(Type, Opts) of
        {query, Query, Fun} ->
            nkactor_store_pgsql:query(SrvId, Query, #{result_fun=>Fun});
        {error, Error} ->
            {error, Error}
    end.
