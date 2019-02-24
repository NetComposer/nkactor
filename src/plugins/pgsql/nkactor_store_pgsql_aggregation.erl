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
-export([aggregation/2]).
-import(nkactor_store_pgsql, [query/2, query/3, quote/1, quote_list/1, filter_path/2]).


-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


-include("nkactor.hrl").



%% ===================================================================
%% API
%% ===================================================================


%% @doc
aggregation(actors_aggregation_groups, Params) ->
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, params, false),
    Query = [
        <<"SELECT \"group\", COUNT(\"group\") FROM actors">>,
        <<" WHERE ">>, filter_path(Namespace, Deep),
        <<" GROUP BY \"group\";">>
    ],
    {query, Query, fun pgsql_aggregation/2};

aggregation(actors_aggregation_resources, Params) ->
    Group = maps:get(group, Params),
    Namespace = maps:get(namespace, Params, <<>>),
    Deep = maps:get(deep, params, false),
    Query = [
        <<"SELECT resource, COUNT(resource) FROM actors">>,
        <<" WHERE \"group\" = ">>, quote(Group), <<" AND ">>, filter_path(Namespace, Deep),
        <<" GROUP BY resource;">>
    ],
    {query, Query, fun pgsql_aggregation/2};


aggregation(AggType, _Params) ->
    {error, {aggregation_not_implemented, AggType}}.



%% ===================================================================
%% Result funs
%% ==================================================================



%%%% @private
%%agg_query(SrvId, Query) ->
%%    case query(SrvId, Query) of
%%        {ok, [Res], Meta} ->
%%            case (catch maps:from_list(Res)) of
%%                {'EXIT', _} ->
%%                    {error, aggregation_invalid};
%%                Map ->
%%                    {ok, Map, Meta}
%%            end;
%%        {error, Error} ->
%%            {error, Error}
%%    end.


%% @private
pgsql_aggregation([{{select, _Size}, Rows, _OpMeta}], Meta) ->
    case (catch maps:from_list(Rows)) of
        {'EXIT', _} ->
            {error, aggregation_invalid};
        Map ->
            {ok, Map, Meta}
    end.
