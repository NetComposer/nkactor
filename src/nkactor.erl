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


%% @doc Actor generic functions
-module(nkactor).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([request/1]).
-export([find/1, activate/1, is_activated/1, update/2, update/3, create/1, create/2, delete/1, delete/2]).
-export([get_actor/1, get_actor/2, get_path/1, is_enabled/1, enable/2, stop/1, stop/2]).
-export([search_groups/2, search_resources/3]).
-export([search_label/3, search_label_range/4, search_linked_to/3, search_fts/3, search_actors/3, delete_multi/3, delete_old/5]).
-export([search_activate/1, truncate/1]).
-export([base_namespace/1, find_label/4, find_linked/3]).
-export([sync_op/2, sync_op/3, async_op/2]).
-export([get_services/0, call_services/2]).
-export_type([actor/0, id/0, uid/0, namespace/0, resource/0, path/0, name/0,
              vsn/0, group/0,hash/0, subresource/0, verb/0,
              data/0, alarm/0]).
-export_type([config/0]).

-include("nkactor.hrl").
-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").


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
            kind => binary(),               % Usually camel version of resource
            subtype => binary(),
            % Version of the actor structure for this group and resource
            vsn => vsn(),
            hash => binary(),
            generation => integer(),
            creation_time => binary(),
            update_time => binary(),
            % If set, this actor will expire and will be deleted on this date
            % Use <<>> to disable
            expires_time => binary(),
            % If set, this actor will be activated and callback will be called
            % Use <<>> to disable
            % System can set it to "0" for a permanent-activated actor
            activate_time => binary(),
            % If value is empty it is removed
            labels => #{binary() => binary()},
            fts => #{binary() => [binary()]},
            % If value is empty it is removed
            links => #{uid() => binary()},
            % If value is empty it is removed
            annotations => #{binary() => binary() | integer() | boolean() | map()},
            is_enabled => boolean(),
            in_alarm => boolean(),
            alarms => [alarm()],
            events => [event()],
            description => binary(),
            trace_id => binary()
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


-type event() :: #{
    class := binary(),
    type => binary(),
    time := binary(),
    meta => map()
}.


-type alarm() :: #{
    class := binary(),
    code := binary(),
    last_time => binary(),
    message => binary(),
    meta => map()
}.


-type config() ::
    #{
        module => module(),                             %% Used for callbacks
        group => group(),
        resource => resource(),
        versions => [vsn()],
        verbs => [atom()],
        permanent => boolean(),                         %% Do not unload
        ttl => integer(),                               %% Unload after msecs
        heartbeat_time => integer(),                    %% msecs for heartbeat
        save_time => integer(),                         %% msecs for auto-save
        activable => boolean(),                         %% Default true
        auto_activate => boolean(),                     %% Auto activate from DB
        async_save => boolean(),
        %% Max number of events stored in actor, default 10
        max_actor_events => integer(),
        %% Max number of alarms stored in actor, default 10
        max_actor_alarms => integer(),
        create_check_unique => boolean(),               %% Default true
        dont_update_on_disabled => boolean(),           %% Default false
        dont_delete_on_disabled => boolean(),           %% Default false
        fields_filter => [nkactor_search:field_name()],
        fields_sort => [nkactor_search:field_name()],
        fields_type => #{
            nkservie_actor_search:field_name() => nkactor_search:field_type()
        },
        fields_static => [nkactor_search:field_name()],  %% Don't allow updates
        camel => binary(),
        singular => binary(),
        short_names => [binary()]
    }.


-type verb() :: atom().


%% @doc Actor Request
%% There are several ways to identify the actor:
%% a) Provide namespace, group, resource and name
%% b) Provide an uid
%% c) Provide a body as map that completes namespace, group, resource and name
%%
%% Name can be omitted in a) and c) if referring to a set of actors
%%
%% Auth field can be used in actor_authorize callback


-type get_opts() ::
    #{
        % False to avoid activating actor
        activate => boolean(),
        % True to delete the actor on read
        consume => boolean(),
        % Use this TTL (if actor is not already loaded)
        ttl => pos_integer(),
        % Do not parse actor's data
        no_data_parse => boolean(),
        % Request will be used when calling actor's parse if provided
        request => nkactor:request(),
        % If ot_span_is defined, logs will be added, and it will be used
        % as parent for new spans that could be created
        ot_span_id => nkserver_ot:span_id() | nkserver_ot:parent()
    }.


-type create_opts() ::
    #{
        % False to avoid activating actor
        activate => boolean(),
        % True to get full actor instead of actor_id
        get_actor => boolean(),
        % Use this TTL (if actor is not already loaded)
        ttl => pos_integer(),
        % If true, no unique check is performed
        no_unique_check => boolean(),
        % Use it only for non-persistent actors, or if sure it is really unique
        forced_uid => binary(),
        request => nkactor:request(),
        ot_span_id => nkserver_ot:span_id() | nkserver_ot:parent()
    }.


-type update_opts() ::
    #{
        merge_data => boolean(),
        request => nkactor:request(),
        get_actor => boolean(),
        allow_name_change => boolean()
    }.


-type delete_opts() ::
    #{
        activate => boolean()
    }.

-type request() :: nkactor_request:request().

-type reply() :: nkactor_request:reply().



%% ===================================================================
%% Public
%% ===================================================================



%% @doc Launches an Request
%% - A new span will be created. If ot_span_id is defined, it will be used as parent
%% - Calls actor_req_authorize/1 for authorization of the request
%% - Finds service managing namespace, and adds srv and start_time

-spec request(request()) ->
    reply().

request(Req) ->
    nkactor_request:request(Req).


%% @doc Finds and actor from UUID or Path, in memory or disk
%% It also checks if it is currently activated, returning the pid
-spec find(id()) ->
    {ok, actor_id()} | {error, actor_not_found|term()}.

find(Id) ->
    case nkactor_backend:find(Id, #{}) of
        {ok, _SrvId, ActorId, _Meta} ->
            {ok, ActorId};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Checks if an actor is currently activated
-spec is_activated(id()) ->
    {true, pid()} | false.

is_activated(Id) ->
    case nkactor_namespace:find_actor(Id) of
        {true, _SrvId, #actor_id{pid=Pid}} when is_pid(Pid) ->
            {true, Pid};
        _ ->
            false
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
        {ok, _SrvId, Actor, _Meta} ->
            {ok, Actor};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Gets an actor's path, activating it first
-spec get_path(id()|pid()) ->
    {ok, path()} | {error, term()}.

get_path(Id) ->
    case nkactor:sync_op(Id, get_actor_id, infinity) of
        {ok, ActorId} ->
            {ok, nkactor_lib:actor_id_to_path(ActorId)};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Check if an actor is enabled, activates first
-spec is_enabled(id()|pid()) ->
    {ok, boolean()} | {error, term()}.

is_enabled(Id) ->
    nkactor:sync_op(Id, is_enabled, infinity).


%% @doc Enables/disabled an object, activates first
-spec enable(id()|pid(), boolean()) ->
    ok | {error, term()}.

enable(Id, Enable) ->
    nkactor:sync_op(Id, {enable, Enable}, infinity).


%% @doc Creates a new actor
-spec create(actor()) ->
    {ok, actor_id()|actor()} | {error, term()}.

create(Actor) ->
    create(Actor, #{}).


%% @doc Creates a new actor
-spec create(actor(), create_opts()) ->
    {ok, actor_id()|actor()} | {error, term()}.

create(Actor, Opts) ->
    case nkactor_backend:create(Actor, Opts) of
        {ok, _SrvId, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Updates an actor, activating it
%% For 'data' field, the full field is replaced, unless option 'merge_data'
%% is used, in that case:
%% - a deep merge is performed
%% - if a key has the value '__op_remove', it is deleted
%%
%% For 'metadata' field, by default all fields remain the same.
%% Some fields can however be updated:
%% - subtype
%% - is_enabled
%% - expires_time (<<>> to remove it)
%% - description
%% - updated_by
%% - labels, annotations, links, fts:
%%      - new values are add or overwrite existing ones
%%      - use value '__op_remove' to remove that key
%%
%% If option 'allow_name_change' is true:
%% - name and namespace can be updated
%% - actor will stop
%% - a check is made so the new name does not conflict, but on non-transactional
%%   databases, an actor could be created in the middle and conflict
%%
%% TODO: Add patch support
%% https://williamdurand.fr/2014/02/14/please-do-not-patch-like-an-idiot/
%% http://erosb.github.io/post/json-patch-vs-merge-patch/

-spec update(id()|pid(), actor()) ->
    {ok, actor()} | {error, term()}.

update(Id, Update) ->
    update(Id, Update, #{}).


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
        {ok, _Meta} ->
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
-spec stop(id()|pid(), Reason::nkserver:status()) ->
    ok | {error, term()}.

stop(Pid, Reason) when is_pid(Pid) ->
    nkactor:async_op(Pid, {stop, Reason});

stop(Id, Reason) ->
    case find(Id) of
        {ok, #actor_id{pid=Pid}} when is_pid(Pid) ->
            stop(Pid, Reason);
        _ ->
            {error, not_activated}
    end.


-type search_opts() :: #{namespace=>namespace(), deep=>boolean()}.

%% @doc Counts classes and objects of each class
-spec search_groups(nkserver:id(), search_opts()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

search_groups(SrvId, Params) ->
    case nkactor_backend:aggregation(SrvId, actors_aggregation_groups, Params) of
        {ok, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.


%% @doc
-spec search_resources(nkserver:id(), group(), search_opts()) ->
    {ok, [{binary(), integer()}], Meta::map()} | {error, term()}.

search_resources(SrvId, Group, Opts) ->
    Params = Opts#{group=>Group},
    case nkactor_backend:aggregation(SrvId, actors_aggregation_resources, Params) of
        {ok, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.



-type search_labels_opts() ::
    #{
        namespace => namespace(),
        deep => boolean(),
        op => nkactor_search:filter_op(),
        value => nkactor_search:value(),
        from => pos_integer(),
        size => pos_integer(),
        only_uid => boolean(),      % If true, Key and Value will be empty
        order => asc | desc
    } | search_opts().


%% @doc Gets objects having a label
-spec search_label(nkservice:id(), binary(), search_labels_opts()) ->
    {ok, [{uid(), Key::binary(), Value::binary()}]} | {error, actor_not_found|term()}.

search_label(SrvId, Label, Opts) ->
    Label2 = nklib_util:to_binary(Label),
    Filter = #{
        'and' => [
            #{
                field => <<"label:", Label2/binary>>,
                op => maps:get(op, Opts, exists),
                value => nklib_util:to_binary(maps:get(value, Opts, <<>>))
            }
        ]
    },
    Sort = case Opts of
        #{order := Order} ->
            [#{field => <<"label:", Label2/binary>>, order=>Order}];
        _ ->
            []
    end,
    Opts2 = maps:with([namespace, deep, from, size], Opts),
    Opts3 = Opts2#{
        filter => Filter,
        sort => Sort,
        only_uid => maps:get(only_uid, Opts, false)
    },
    Opts4 = case maps:is_key(namespace, Opts3) of
        true ->
            Opts3;
        false ->
            Opts3#{namespace => base_namespace(SrvId)}
    end,
    case nkactor_backend:search(SrvId, actors_search_labels, Opts4) of
        {ok, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.


-type search_labels_range_opts() ::
    #{
        namespace => namespace(),
        deep => boolean(),
        include_first => boolean(),
        include_last => boolean(),
        size => pos_integer(),
        only_uid => boolean(),      % If true, Key and Value will be empty
        order => asc | desc
    } | search_opts().


%% @doc Gets objects having a label
-spec search_label_range(nkservice:id(), binary(), binary(), search_labels_range_opts()) ->
    {ok, [{uid(), Key::binary(), Value::binary()}]} | {error, actor_not_found|term()}.

search_label_range(SrvId, Start, Stop, Opts) ->
    Label1 = nklib_util:to_binary(Start),
    Label2 = nklib_util:to_binary(Stop),
    Filter = #{
        'and' => [
            #{
                field => <<"label:", Label1/binary>>,
                op => case Opts of #{include_first:=true} -> first; _ -> top end,
                value => <<>>
            },
            #{
                field => <<"label:", Label2/binary>>,
                op => case Opts of #{include_last:=true} -> last; _ -> bottom end,
                value => <<>>
            }
        ]

    },
    Order = maps:get(order, Opts, asc),
    Sort = [
        #{field => <<"label_key">>, order=>Order},
        #{field => <<"label_value">>, order=>Order}
    ],
    Opts2 = maps:with([namespace, deep, from, size], Opts),
    Opts3 = Opts2#{
        filter => Filter,
        sort => Sort,
        only_uid => maps:get(only_uid, Opts, false)
    },
    Opts4 = case maps:is_key(namespace, Opts3) of
        true ->
            Opts3;
        false ->
            Opts3#{namespace => base_namespace(SrvId)}
    end,
    case nkactor_backend:search(SrvId, actors_search_labels, Opts4) of
        {ok, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.



-type search_linked_opts() ::
    #{
        link_type => binary(),
        from => pos_integer(),
        size => pos_integer()
    } | search_opts().

%% @doc Gets objects pointing to another
-spec search_linked_to(nkservice:id(), id(),search_linked_opts()) ->
    {ok, [{UID::binary(), LinkType::binary()}]} | {error, term()}.

search_linked_to(SrvId, Id, Opts) ->
    case find(Id) of
        {ok, #actor_id{uid=UID}} ->
            Params = Opts#{uid => UID},
            case nkactor_backend:search(SrvId, actors_search_linked, Params) of
                {ok, Result, _Meta} ->
                    {ok, Result};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


-type search_fts_opts() ::
    #{
        field => binary(),
        from => pos_integer(),
        size => pos_integer()
    } | search_opts().


%% @doc Gets objects under a path, sorted by path
-spec search_fts(nkservce:id(), binary(), search_fts_opts()) ->
    {ok, [UID::binary()]} | {error, term()}.

search_fts(SrvId, Word, Opts) ->
    Params = Opts#{word => Word},
    case nkactor_backend:search(SrvId, actors_search_fts, Params) of
        {ok, Result, _Meta} ->
            {ok, Result};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Generic search returning actors
%% If 'meta' is not populated with filters available, types, etc.:
%% - any field can be used for filter, sort, etc.
%% - you must supply the type, or it will be converted to string
-spec search_actors(nkserver:id(), nkactor_search:spec(), nkactor_seach:opts()) ->
    {ok, [actor()], Meta::map()} | {error, term()}.

search_actors(SrvId, SearchSpec, SearchOpts) ->
    case nkactor_search:parse_spec(SearchSpec, SearchOpts) of
        {ok, SearchSpec2} ->
            %lager:error("NKLOG PARSED ~p", [SearchSpec2]),
            case nkactor_backend:search(SrvId, actors_search, SearchSpec2) of
                {ok, Actors, Meta} ->
                    {ok, Actors, maps:with([size, total], Meta)};
                {error, Error} ->
                    {error, Error}
            end;
        {error, Error} ->
            {error, Error}
    end.


%% @doc Generic deletion of objects
%% If will find all actors, and delete them, along with their dependencies
-spec delete_multi(nkserver:id(), nkactor_search:spec(), nkactor_search:opts()) ->
    {ok, #{deleted:=integer()}} | {error, term()}.

delete_multi(SrvId, SearchSpec, SearchOpts) ->
    case search_actors(SrvId, SearchSpec, SearchOpts) of
        {ok, Actors, _Meta} ->
            ActorIds = [nkactor_lib:actor_to_actor_id(Actor) || Actor <- Actors],
            nkactor_backend:delete_multi(SrvId, ActorIds);
        {error, Error} ->
            {error, Error}
    end.


%% @doc Deletes actors older than Epoch (secs)
-spec delete_old(nkserver:id(), group(), resource(), integer(), search_opts()) ->
    {ok, integer(), Meta::map()}.

delete_old(SrvId, Group, Res, Date, Opts) ->
    Params = Opts#{group=>Group, resource=>Res, epoch=>Date},
    nkactor_backend:search(SrvId, actors_delete_old, Params).


%% @doc Find actors with activation date < current date + 2h (or use last_time)
%% If 2 actors share the exact same activation date (in usecs) and pagination
%% stops on it, some could be lost
%% Since we are activating 2h in advance, next time should't happen
-spec search_activate(nkactor:id()) ->
    {ok, [#actor_id{}]} | {error, term()}.

search_activate(SrvId) ->
    Now = nklib_date:epoch(usecs),
    Time1 = Now + 2 * 60 * 60 * 1000 * 1000,
    {ok, Time2} = nklib_date:to_3339(Time1, usecs),
    nkactor_backend:search_activate_actors(SrvId, Time2, 1000).


%%%% @doc
%%%% Use last_cursor as initial date to expire
%%-spec search_expired(nkactor:id(), #{last_cursor=>binary, size=>integer()}) ->
%%    {ok, [uid()], #{last_cursor=>binary()}}.
%%
%%search_expired(SrvId, Opts) ->
%%    nkactor_backend:search(SrvId, actors_expired, Opts).


%% @doc
base_namespace(undefined) ->
    undefined;

base_namespace(SrvId) ->
    nkserver:get_cached_config(SrvId, nkactor, base_namespace).


%% @doc DELETES ALL ACTORS!
truncate(SrvId) ->
    nkactor_backend:truncate(SrvId, #{}).


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


%% @doc Finds first actor with some label, and stores it in cache
%% Only UID will be populated, along with pid if available
-spec find_label(nkserver:id(), namespace(), binary(), binary()) ->
    {ok, #actor_id{}} | {error, term()}.

find_label(SrvId, Namespace, Key, Value) ->
    Key2 = nklib_util:to_binary(Key),
    Value2 = nklib_util:to_binary(Value),
    case nklib_proc:values({?MODULE, label, SrvId, Key2, Value2}) of
        [{UID, Pid}|_] ->
            {ok, #actor_id{uid=UID, pid=Pid}};
        [] ->
            Opts = #{
                op => eq,
                value => Value2,
                order => desc,
                namespace => Namespace,
                deep => true
                %ot_span_id=>Parent
            },
            case search_label(SrvId, Key, Opts) of
                {ok, [{UID, _, _}|_]} ->
                    case is_activated(UID) of
                        {true, Pid} ->
                            nklib_proc:put({?MODULE, label, SrvId, Key2, Value2}, UID, Pid),
                            {ok, #actor_id{uid=UID, pid=Pid}};
                        _ ->
                            {ok, #actor_id{uid=UID}}
                    end;
                {ok, []} ->
                    {error, {label_not_found, Key}};
                {error, Error} ->
                    {error, Error}
            end
    end.


%% @doc Finds first actor linked to another, and stores it in cache
-spec find_linked(nkserver:id(), binary(), uid()) ->
    {ok, uid(), pid()|undefined} | {error, term()}.

find_linked(SrvId, Type, UID) ->
    Type2 = nklib_util:to_binary(Type),
    case nklib_proc:values({?MODULE, linked, SrvId, Type2, UID}) of
        [{UID2, Pid}|_] ->
            {ok, UID2, Pid};
        [] ->
            Opts = #{
                link_type => Type2,
                deep => true,
                size => 1
            },
            case search_linked_to(SrvId, UID, Opts) of
                {ok, [{UID2, _Type}|_]} ->
                    case is_activated(UID2) of
                        {true, Pid} ->
                            nklib_proc:put({?MODULE, linked, SrvId, Type2, UID}, UID2, Pid),
                            {ok, UID2, Pid};
                        _ ->
                            {ok, UID2, undefined}
                    end;
                {ok, []} ->
                    {error, actor_not_found};
                {error, Error} ->
                    {error, Error}
            end
    end.




%% @doc Calls all defined services for a callback
%% If it returns 'continue', next service will be tried
call_services(Fun, Args) ->
    call_services(get_services(), Fun, Args).

call_services([], _Fun, _Args) ->
    continue;

call_services([SrvId|Rest], Fun, Args) ->
    case ?CALL_SRV(SrvId, Fun, [SrvId|Args]) of
        continue ->
            call_services(Rest, Fun, Args);
        Other ->
            Other
    end.


%% @doc
get_services() ->
    [
        SrvId ||
        {SrvId, _Hash, _Pid} <- nkserver_srv:get_all_local(nkactor)
    ].


