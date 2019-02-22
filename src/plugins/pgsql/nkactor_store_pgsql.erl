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

-module(nkactor_store_pgsql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([query/2, query/3]).
-export([quote/1, quote_list/1]).
-export_type([result_fun/0]).


-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


%% ===================================================================
%% Types
%% ===================================================================

-type result_fun() :: fun(([[tuple()]], map()) -> {ok, term(), map()} | {error, term()}).


%% ===================================================================
%% API
%% ===================================================================

% https://www.cockroachlabs.com/docs/stable/
% https://www.cockroachlabs.com/docs/dev/

%% @doc Performs a query
-spec query(nkserver:id(), binary()|nkpgsql:query_fun()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query) ->
    PgSrvId = nkserver:get_plugin_config(SrvId, nkactor_store_pgsql, pgsql_service),
    nkpgsql:query(PgSrvId, Query, #{}).


%% @doc Performs a query
-spec query(nkserver:id(), binary()|nkpgsql:query_fun(), nkpgsql:query_meta()) ->
    {ok, list(), Meta::map()} |
    {error, {pgsql_error, nkpgsql:pgsql_error()}|term()}.

query(SrvId, Query, QueryMeta) ->
    PgSrvId = nkserver:get_plugin_config(SrvId, nkactor_store_pgsql, pgsql_service),
    nkpgsql:query(PgSrvId, Query, QueryMeta).


quote(Term) ->
    nkpgsql_util:quote(Term).

quote_list(Term) ->
    nkpgsql_util:quote_list(Term).