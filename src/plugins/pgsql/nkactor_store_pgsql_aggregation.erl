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

-module(nkactor_store_pgsql_aggregation).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([aggregation/3]).
-import(nkactor_store_pgsql, [query/2, query/3, quote/1, quote_list/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


-include("nkactor.hrl").



%% ===================================================================
%% API
%% ===================================================================


%% @doc
aggregation(_SrvId, _SearchType, _Opts) ->
    {error, aggregation_invalid}.

%%    case ?CALL_SRV(SrvId, actor_db_get_query, [SrvId, pgsql, SearchType, Opts]) of
%%        {ok, {pgsql, Query, QueryMeta}} ->
%%            case query(SrvId, Query, QueryMeta) of
%%                {ok, [Res], Meta} ->
%%                    case (catch maps:from_list(Res)) of
%%                        {'EXIT', _} ->
%%                            {error, aggregation_invalid};
%%                        Map ->
%%                            {ok, Map, Meta}
%%                    end;
%%                {ok, _, _} ->
%%                    {error, aggregation_invalid};
%%                {error, Error} ->
%%                    {error, Error}
%%            end;
%%        {error, Error} ->
%%            {error, Error}
%%    end.
%%
