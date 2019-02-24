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

-module(nkactor_store_pgsql_namespaces).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([get_namespace/2, update_namespace/5]).
-import(nkactor_store_pgsql, [query/2, query/3, quote/1, quote_list/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


-include("nkactor.hrl").

%% ===================================================================
%% Types
%% ===================================================================


%% ===================================================================
%% API
%% ===================================================================



%% @doc
get_namespace(SrvId, Namespace) ->
    Query = [
        <<"SELECT service,cluster,last_update,data FROM namespaces ">>,
        <<" WHERE namespace=">>, quote(Namespace), <<";">>
    ],
    case query(SrvId, Query) of
        {ok, [[Fields]], QueryMeta} ->
            {Service, Cluster, Updated, Data} = Fields,
            Reply = #{
                namespace => Namespace,
                service => Service,
                cluster => Cluster,
                updated => Updated,
                data => nklib_json:decode(Data)
            },
            {ok, Reply, QueryMeta};
        {ok, [[]], _QueryMeta} ->
            {error, {namespace_not_found, Namespace}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
update_namespace(SrvId, Namespace, Srv, Cluster, Data) ->
    Update = nklib_date:now_3339(secs),
    Query = [
        <<"UPSERT INTO namespaces (namespace,service,cluster,last_update,data) VALUES (">>,
        quote(Namespace), $,, quote(Srv), $,,  quote(Cluster), $,,
        quote(Update), $,, quote(Data), <<");">>
    ],
    case query(SrvId, Query) of
        {ok, _, QueryMeta} ->
            {ok, QueryMeta};
        {error, Error} ->
            {error, Error}
    end.
