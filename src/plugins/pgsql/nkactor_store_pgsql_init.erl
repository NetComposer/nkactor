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

-module(nkactor_store_pgsql_init).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([init/1, init/2, drop/1]).

-define(LLOG(Type, Txt, Args), lager:Type("NkACTOR PGSQL "++Txt, Args)).


%% ===================================================================
%% API
%% ===================================================================

%% @private
init(SrvId) ->
    init(SrvId, 10).


%% @private
init(SrvId, Tries) when Tries > 0 ->
    case nkactor_store_pgsql:query(SrvId, <<"SELECT id,version FROM versions">>) of
        {ok, [Rows], _} ->
            case maps:from_list(Rows) of
                #{
                    <<"actors">> := ActorsVsn,
                    <<"links">> := LinksVsn,
                    <<"labels">> := LabelsVsn,
                    <<"fts">> := FtsVsn,
                    <<"namespaces">> := NamespacesVsn
                } ->
                    case {ActorsVsn, LinksVsn, LabelsVsn, FtsVsn, NamespacesVsn} of
                        {<<"1">>, <<"1">>, <<"1">>, <<"1">>, <<"1">>} ->
                            ?LLOG(notice, "detected database at last version", []),
                            ok;
                        _ ->
                            ?LLOG(warning, "detected database at wrong version", []),
                            ok
                    end;
                _ ->
                    ?LLOG(error, "unrecognized database!", []),
                    {error, database_unrecognized}
            end;
        {error, relation_unknown} ->
            ?LLOG(warning, "database not found: Creating it", []),
            case nkactor_store_pgsql:query(SrvId, create_database_query()) of
                {ok, _, _} ->
                    ok;
                {error, Error} ->
                    ?LLOG(warning, "Could not create database: ~p", [Error]),
                    {error, Error}
            end;
        {error, Error} ->
            ?LLOG(notice, "could not create database: ~p (~p tries left)", [Error, Tries]),
            timer:sleep(1000),
            init(SrvId, Tries-1)
    end;

init(_SrvId, _Tries) ->
    {error, database_not_available}.



%% @private
drop(SrvId) ->
    Q = <<"
        DROP TABLE IF EXISTS versions CASCADE;
        DROP TABLE IF EXISTS actors CASCADE;
        DROP TABLE IF EXISTS links CASCADE;
        DROP TABLE IF EXISTS labels CASCADE;
        DROP TABLE IF EXISTS fts CASCADE;
        DROP TABLE IF EXISTS namespaces CASCADE;
    ">>,
    case nkactor_store_pgsql:query(SrvId, Q) of
        {ok, _, _} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.


%% @private
create_database_query() ->
    <<"
        BEGIN;
        CREATE TABLE versions (
            id STRING PRIMARY KEY NOT NULL,
            version STRING NOT NULL
        );
        CREATE TABLE actors (
            uid STRING PRIMARY KEY NOT NULL,
            \"group\" STRING NOT NULL,
            resource STRING NOT NULL,
            name STRING NOT NULL,
            namespace STRING NOT NULL,
            data JSONB NOT NULL,
            metadata JSONB NOT NULL,
            path STRING NOT NULL,
            hash STRING NOT NULL,
            last_update STRING NOT NULL,
            expires INTEGER,
            fts_words STRING,
            UNIQUE INDEX name_idx (namespace, \"group\", resource, name),
            INDEX last_update_idx (last_update),
            INDEX expires_idx (expires),
            INVERTED INDEX data_idx (data),
            INVERTED INDEX metadata_idx (metadata)
        );
        INSERT INTO versions VALUES ('actors', '1');
        CREATE TABLE labels (
            uid STRING NOT NULL REFERENCES actors(uid) ON DELETE CASCADE,
            label_key STRING NOT NULL,
            label_value STRING NOT NULL,
            path STRING NOT NULL,
            PRIMARY KEY (uid, label_key),
            UNIQUE INDEX label_idx (label_key, uid)
        ) INTERLEAVE IN PARENT actors(uid);
        INSERT INTO versions VALUES ('labels', '1');
        CREATE TABLE links (
            uid STRING NOT NULL REFERENCES actors(uid) ON DELETE CASCADE,
            link_target STRING NOT NULL REFERENCES actors(uid) ON DELETE CASCADE,
            link_type STRING NOT NULL,
            path STRING NOT NULL,
            PRIMARY KEY (uid, link_target, link_type),
            UNIQUE INDEX link_idx (link_target, link_type, uid)
        ) INTERLEAVE IN PARENT actors(uid);
        INSERT INTO versions VALUES ('links', '1');
        CREATE TABLE fts (
            uid STRING NOT NULL REFERENCES actors(uid) ON DELETE CASCADE,
            fts_word STRING NOT NULL,
            fts_field STRING NOT NULL,
            path STRING NOT NULL,
            PRIMARY KEY (uid, fts_word, fts_field),
            UNIQUE INDEX fts_idx (fts_word, fts_field, uid)
        )  INTERLEAVE IN PARENT actors(uid);
        INSERT INTO versions VALUES ('fts', '1');
        CREATE TABLE namespaces (
            namespace STRING PRIMARY KEY NOT NULL,
            service STRING NOT NULL,
            cluster STRING NOT NULL,
            last_update STRING NOT NULL,
            data JSONB NOT NULL
        );
        INSERT INTO versions VALUES ('namespaces', '1');
        COMMIT;
    ">>.

