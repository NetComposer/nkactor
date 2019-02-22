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


%% @doc Basic Actor behaviour
-module(nkactor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([find/1, activate/1, update/3, create/2, delete/1, delete/2]).
-export([get_actor/1, get_actor/2, get_path/1, is_enabled/1, enable/2, stop/1, stop/2]).
-export([search_groups/3, search_resources/4]).
-export([search_linked_to/5, search_fts/5, search/3, search_ids/3,
         delete_all/3, delete_old/6]).
-export([request/1, sync_op/2, sync_op/3, async_op/2]).
-export_type([actor/0, id/0, uid/0, namespace/0, resource/0, path/0, name/0,
              vsn/0, group/0,hash/0,
              data/0, alarm_class/0, alarm_body/0]).
-export_type([config/0]).
-export_type([request/0, response/0]).

-include("nkactor.hrl").
-include("nkactor.hrl").
-include("nkactor_debug.hrl").



%% ===================================================================
%% Callbacks definitions
%% ===================================================================


-type actor() ::
    #{
        uid => uid(),
        group => group(),
        resource => resource(),
        name => name(),
        namespace => namespace(),
        data => map(),
        metadata => #{
            subtype => binary(),
            vsn => vsn(),
            hash => binary(),
            generation => integer(),
            creation_time => binary(),
            update_time => binary(),
            is_active => boolean(),         % must be loaded at all times
            expires_time => binary(),
            labels => #{binary() => binary() | integer() | boolean()},
            fts => #{binary() => [binary()]},
            links => #{uid() => binary},
            annotations => #{binary() => binary() | integer() | boolean() | map()},
            is_enabled => boolean(),
            in_alarm => boolean(),
            alarms => [alarm()],
            next_status_time => boolean(),
            description => binary()
        }
    }.



-type actor_id() :: #actor_id{}.

-type id() :: path() | uid() | actor_id().

-type uid() :: binary().

-type namespace() :: binary().

-type group() :: binary().

-type vsn() :: binary().

-type hash() :: binary().

-type resource() :: binary().

-type subresource() :: binary().


-type path() :: binary().   %% Group:Resource:Name.namespace

-type name() :: binary().

-type data() ::
    #{
        binary() => binary() | integer() | float() | boolean()
    }.



-type alarm() :: term().

-type alarm_class() :: binary().

%% Recommended alarm fields
%% ------------------------
%% - code (binary)
%% - message (binary)
%% - lastTime (binary, rfc3339)
%% - meta (map)

-type alarm_body() :: map().




-type config() ::
    #{
        module => module(),                             %% Used for callbacks
        group => group(),
        resource => resource(),
        versions => [vsn()],
        verbs => [atom()],
        permanent => boolean(),                         %% Do not unload
        ttl => integer(),                               %% Unload after msecs
        save_time => integer(),                         %% msecs for auto-save
        activable => boolean(),                         %% Default true
        dont_update_on_disabled => boolean(),           %% Default false
        dont_delete_on_disabled => boolean(),           %% Default false
        immutable_fields => [nkactor_search:field_name()],  %% Don't allow updates
        auto_activate => boolean(),                     %% Periodic automatic activation
        async_save => boolean(),
        filter_fields => [nkactor_search:field_name()],
        sort_fields => [nkactor_search:field_name()],
        field_type => #{
            nkservie_actor_search:field_name() => nkactor_search:field_type()
        },

        camel => binary(),
        singular => binary(),
        short_names => [binary()]
    }.


-type verb() :: atom().


-type request() ::
    #{
        verb => verb(),
        group => group(),
        namespace => namespace(),
        resource => resource(),
        name => nkservice_actor:name(),
        subresource => subresource(),
        params => #{binary() => binary()},
        body => term(),
        auth => map(),
        callback => module(),           % Implementing nkdomain_api behaviour
        external_url => binary(),       % External url to use in callbacks
        srv => nkservice:id(),          % Service that received the request
        start_time => nklib_date:epoch(usecs),
        trace => [{Time::integer(), Op::term(), Meta::map()}],
        meta => map()
    }.

-type response() ::
    ok | {ok, map()} | {ok, map(), request()} |
    created | {created, map()} | {created, map(), request()} |
    {status, nkserver:msg()} |
    {status, nkserver:msg(), map()} |  %% See status/2
    {status, nkserver:msg(), map(), request()} |
    {error, nkserver:msg()} |
    {error, nkserver:msg(), request()}.


-type get_opts() ::
    #{
        activate => boolean(),
        consume => boolean(),
        ttl => pos_integer()
    }.


-type create_opts() ::
    #{
        activate => boolean(),
        ttl => pos_integer(),
        request => request()
    }.


-type update_opts() ::
    #{
        data_fields => [binary()],          % If used, fields not defined here will not be updated
        request => request()
    }.


-type delete_opts() ::
    #{
        activate => boolean()
    }.


%% ===================================================================
%% Public
%% ===================================================================


%% @doc Finds and actor from UUID or Path, in memory or disk
%% It also checks if it is currently activated, returning the pid
-spec find(id()) ->
    {ok, nkserver:id(), actor_id(), Meta::map()} | {error, actor_not_found|term()}.

find(Id) ->
    case nkactor_backend:find(Id) of
        {ok, _SrvId, ActorId, _Meta} ->
            {ok, ActorId};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Finds an actors's pid or loads it from storage and activates it
-spec activate(id()) ->
    {ok, actor_id()} | {error, actor_not_found|term()}.

activate(Id) ->
    case nkactor_backend:activate(Id, #{}) of
        {ok, _SrvId, ActorId, _Meta} ->
            {ok, ActorId};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Read an actor, activating it first
-spec get_actor(id()|pid()) ->
    {ok, actor()} | {error, term()}.

get_actor(Id) ->
    get_actor(Id, #{}).


%% @doc Read an actor, activating it first
-spec get_actor(id()|pid(), get_opts()) ->
    {ok, actor()} | {error, term()}.

get_actor(Id, Opts) ->
    case nkactor_backend:read(Id, Opts) of
        {ok, _SrvId, ActorId, _Meta} ->
            {ok, ActorId};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets an actor's path, activating it first
-spec get_path(id()|pid()) ->
    {ok, path()} | {error, term()}.

get_path(Id) ->
    case nkactor_srv:sync_op(Id, get_actor_id) of
        {ok, ActorId} ->
            {ok, nkactor_lib:actor_id_to_path(ActorId)};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Check if an actor is enabled, activates first
-spec is_enabled(id()|pid()) ->
    {ok, boolean()} | {error, term()}.

is_enabled(Id) ->
    nkactor_srv:sync_op(Id, is_enabled).


%% @doc Enables/disabled an object, activates first
-spec enable(id()|pid(), boolean()) ->
    ok | {error, term()}.

enable(Id, Enable) ->
    nkactor_srv:sync_op(Id, {enable, Enable}).


%% @doc Creates a new actor
-spec create(actor(), create_opts()) ->
    {ok, actor()} | {error, term()}.

create(Actor, Opts) ->
    case nkactor_backend:create(Actor, Opts) of
        {ok, _SrvId, Actor2, _Meta} ->
            {ok, Actor2};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Updates an actor, activating it
-spec update(id()|pid(), actor(), update_opts()) ->
    {ok, actor()} | {error, term()}.

update(Id, Update, Opts) ->
    case nkactor_backend:update(Id, Update, Opts) of
        {ok, _SrvId, Actor2, _Meta} ->
            {ok, Actor2};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Remove an actor
-spec delete(id()|pid()) ->
    ok | {error, term()}.

delete(Id) ->
    delete(Id, #{}).


%% @doc Remove an actor
-spec delete(id()|pid(), delete_opts()) ->
    ok | {error, term()}.

delete(Id, Opts) ->
    case nkactor_backend:delete(Id, Opts) of
        {ok, _ActorIds, _Meta} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Unloads an actor
-spec stop(id()|pid()) ->
    ok | {error, term()}.

stop(Id) ->
    stop(Id, normal).


%% @doc Unloads the object
-spec stop(id()|pid(), Reason::nkserver:msg()) ->
    ok | {error, term()}.

stop(Pid, Reason) when is_pid(Pid) ->
    nkactor_srv:async_op(Pid, {stop, Reason});

stop(Id, Reason) ->
    case find(Id) of
        {ok, #actor_id{pid=Pid}} when is_pid(Pid) ->
            stop(Pid, Reason);
        _ ->
            {error, not_activated}
    end.


%% @doc Counts classes and objects of each class
-spec search_groups(id(), namespace(), #{deep=>boolean()}) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

search_groups(SrvId, Domain, Opts) ->
    nkactor_backend:aggregation(SrvId, {service_aggregation_groups, Domain, Opts}).


%% @doc
-spec search_resources(id(), group(), namespace(), #{deep=>boolean()}) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

search_resources(SrvId, Domain, Group, Opts) ->
    nkactor_backend:aggregation(SrvId, {service_aggregation_resources, Domain, Group, Opts}).


%% @doc Gets objects pointing to another
-spec search_linked_to(id(), namespace(), id(), binary()|any,
                       #{deep=>boolean(), from=>pos_integer(),size=>pos_integer()}) ->
    {ok, [{UID::binary(), LinkType::binary()}]} | {error, term()}.

search_linked_to(SrvId, Domain, Id, LinkType, Opts) ->
    case nkactor_backend:find(Id) of
        {ok, SrvId, #actor_id{uid=UID}, _} ->
            nkactor_backend:search(SrvId, {service_search_linked, Domain, UID, LinkType, Opts});
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets objects under a path, sorted by path
-spec search_fts(nkservce:id(), namespace(), binary()|any, binary(),
    #{deep=>boolean(), from=>pos_integer(),size=>pos_integer()}) ->
    {ok, [UID::binary()], Meta::map()} | {error, term()}.

search_fts(SrvId, Domain, Field, Word, Opts) ->
    nkactor_backend:search(SrvId, {service_search_fts, Domain, Field, Word, Opts}).


%% @doc Generic search returning actors
-spec search(id(), nkactor_search:search_spec(),
             nkactor_search:search_opts()) ->
    {ok, [actor()], Meta::map()} | {error, term()}.

search(SrvId, SearchSpec, SearchOpts) ->
    case nkactor_search:parse(SearchSpec, SearchOpts) of
        {ok, SearchSpec2} ->
            nkactor_backend:search(SrvId, {service_search_actors, SearchSpec2});
        {error, Error} ->
            {error, Error}
    end.


%% @doc Generic search returning actors
%% Meta will includesize, last_updated and total (if not totals=false)
-spec search_ids(id(), nkactor_search:search_spec(),
    nkactor_search:search_opts()) ->
    {ok, [actor_id()], Meta::map()} | {error, term()}.

search_ids(SrvId, SearchSpec, SearchOpts) ->
    case nkactor_search:parse(SearchSpec, SearchOpts) of
        {ok, SearchSpec2} ->
            nkactor_backend:search(SrvId, {service_search_actors_id, SearchSpec2});
        {error, Error} ->
            {error, Error}
    end.


%% @doc Deletes actors older than Epoch (secs)
-spec delete_old(id(), namespace(), group(), resource(), binary(), #{deep=>boolean()}) ->
    {ok, integer(), Meta::map()}.

delete_old(SrvId, Domain, Group, Type, Date, Opts) ->
    nkactor_backend:search(SrvId, {service_delete_old_actors, Domain, Group, Type, Date, Opts}).


%% @doc Generic deletion of objects
%% Use delete=>true for real deletion
%% Use search_opts() to be able to use special fields, otherwise anything is accepted
-spec delete_all(id(), nkactor_search:search_spec()|#{delete=>boolean()},
                 nkactor_search:search_opts()) ->
    {ok|deleted, integer(), Meta::map()}.

delete_all(SrvId, SearchSpec, SearchOpts) ->
    {Delete, SearchSpec2} = case maps:take(delete, SearchSpec) of
        error ->
            {false, SearchSpec};
        {Test0, SearchSpec0} ->
            {Test0, SearchSpec0}
    end,
    case nkactor_search:parse(SearchSpec2, SearchOpts) of
        {ok, SearchSpec3} ->
            case nkactor_backend:search(SrvId, {service_delete_actors, Delete, SearchSpec3}) of
                {ok, Total, Meta} when Delete ->
                    {deleted, Total, Meta};
                {ok, Total, Meta} ->
                    {ok, Total, Meta};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Launches an Request
-spec request(nkactor:request()) ->
    nkactor:response().

request(Req) ->
    nkactor_request:request(Req).


%% @doc
-spec sync_op(nkactor:id()|pid(), nkactor_srv:sync_op()) ->
    term() | {error, timeout|process_not_found|actor_not_found|term()}.

sync_op(Id, Op) ->
    nkactor_srv:sync_op(Id, Op).


%% @doc
-spec sync_op(nkactor:id()|pid(), nkactor_srv:sync_op(), nkactor_srv:timeout()) ->
    term() | {error, timeout|process_not_found|actor_not_found|term()}.

sync_op(Id, Op, Timeout) ->
    nkactor_srv:sync_op(Id, Op, Timeout).


%% @doc
-spec async_op(nkactor:id()|pid(), nkactor_srv:async_op()) ->
    ok | {error, process_not_found|actor_not_found|term()}.

async_op(Id, Op) ->
    nkactor_srv:async_op(Id, Op).


