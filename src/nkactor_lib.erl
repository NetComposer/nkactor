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
-module(nkactor_lib).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-include("nkactor.hrl").
-include("nkactor_debug.hrl").
-include_lib("nkserver/include/nkserver.hrl").

-export([actor_to_actor_id/1, id_to_actor_id/1, id_to_actor_id/2]).
-export([send_external_event/3]).
-export([get_linked_type/2, get_linked_uids/2, add_link/3, add_link/4, add_link/5, link_type/2]).
-export([add_creation_fields/1, update/2, check_links/1, do_check_links/2]).
-export([add_labels/4, add_label/3]).
-export([actor_id_to_path/1]).
-export([parse/2, parse/3, parse_actor_data/2, parse_request_params/2]).
-export([make_rev_path/1, make_rev_parts/1]).
-export([make_plural/1, make_singular/1, normalized_name/1]).
-export([fts_normalize_word/1, fts_normalize_multi/1]).
-export([update_check_fields/2]).


-type actor() :: nkactor:actor().

%% ===================================================================
%% Public
%% ===================================================================


%% @doc
actor_id_to_path(#actor_id{namespace=Namespace, group=Group, resource=Res, name=Name}) ->
    list_to_binary([Group, $:, Res, $:, Name, $., Namespace]).



%% @doc
-spec actor_to_actor_id(actor()) ->
    #actor_id{}.

actor_to_actor_id(Actor) ->
    #{
        group := Group,
        resource := Resource,
        name := Name,
        namespace := Namespace
    } = Actor,
    #actor_id{
        group = Group,
        resource = Resource,
        name = Name,
        %vsn = maps:get(vsn, Actor, undefined),
        namespace = Namespace,
        uid = maps:get(uid, Actor, undefined),
        pid = undefined
    }.


%% @doc Canonizes id to #actor_id{}
-spec id_to_actor_id(nkactor:id()) ->
    #actor_id{}.

id_to_actor_id(#actor_id{}=ActorId) ->
    ActorId;

id_to_actor_id(Path) ->
    id_to_actor_id(undefined, Path).


%% @doc Canonizes id to #actor_id{}
-spec id_to_actor_id(nkserver:id(), nkactor:id()) ->
    #actor_id{}.

id_to_actor_id(SrvId, #actor_id{namespace=undefined}=ActorId) when SrvId /= undefined ->
    Base = nkactor:base_namespace(SrvId),
    ActorId#actor_id{namespace = Base};

id_to_actor_id(_SrvId, #actor_id{}=ActorId) ->
    ActorId;

id_to_actor_id(SrvId, Path) ->
    case to_bin(Path) of
        <<$/, Api/binary>> ->
            case binary:split(Api, <<"/">>, [global]) of
                [<<"apis">>, Group, _Vsn, <<"namespaces">>, Namespace, Resource, Name] ->
                    <<Group/binary, $:, Resource/binary, $:, Name/binary, $., Namespace/binary>>;
                [<<"apis">>, Group, _Vsn, Resource, Name] when SrvId /= undefined ->
                    Base = nkactor:base_namespace(SrvId),
                    <<Group/binary, $:, Resource/binary, $:, Name/binary, $., Base/binary>>;
                _ ->
                    Path
            end;
        Path2 ->
            case binary:split(to_bin(Path2), <<$.>>) of
                [FullName, Namespace] ->
                    case binary:split(FullName, <<$:>>, [global]) of
                        [Group, Resource, Name] ->
                            #actor_id{
                                group = Group,
                                resource = Resource,
                                name = Name,
                                namespace = Namespace
                            };
                        [Resource, Name] ->
                            #actor_id{
                                resource = Resource,
                                name = Name,
                                namespace = Namespace
                            };
                        [Name] ->
                            #actor_id{
                                name = Name,
                                namespace = Namespace
                            }
                    end;
                [UID] ->
                    #actor_id{uid = UID}
            end
    end.


%% @doc
add_link(#actor_id{uid=UID}, #{}=Actor, LinkType) ->
    #{metadata:=Meta} = Actor,
    Links1 = maps:get(links, Meta, #{}),
    Links2 = Links1#{UID => LinkType},
    Actor#{metadata:=Meta#{links => Links2}}.


%% @doc
add_link(Id, Group, Resource, #{}=Actor) when is_binary(Group), is_binary(Resource) ->
    LinkType = link_type(Group, Resource),
    add_link(Id, Group, Resource, Actor, LinkType).


%% @doc
add_link(Id, Group, Resource, #{}=Actor, LinkType)
        when is_binary(Group), is_binary(Resource), is_binary(LinkType) ->
    case nkactor:find(Id) of
        {ok, #actor_id{group=Group, resource=Resource, uid=UID}} ->
            #{metadata:=Meta} = Actor,
            Links1 = maps:get(links, Meta, #{}),
            Links2 = Links1#{UID => LinkType},
            {ok, Actor#{metadata:=Meta#{links => Links2}}};
        _ ->
            {error, {actor_not_found, Id}}
    end.


%% @doc
get_linked_type(UID, #{metadata:=#{links:=Links}}) ->
    maps:get(UID, Links, <<>>);

get_linked_type(_UID, _) ->
    <<>>.


%% @doc Finds all linked objects with this type
get_linked_uids(Type, #{metadata:=Meta}) ->
    maps:fold(
        fun(UID, FunType, Acc) ->
            case Type==FunType of
                true -> [UID|Acc];
                false -> Acc
            end
        end,
        [],
        maps:get(links, Meta, #{})).


%% @doc
link_type(Group, Resource) ->
    <<"io.netc.", Group/binary, $., Resource/binary>>.


%% @doc
add_labels(Prefix, List, Value, #{metadata:=Meta}=Actor) ->
    Labels1 = maps:get(labels, Meta, #{}),
    Labels2 = lists:foldl(
        fun(Term, Acc) ->
            Key = list_to_binary([Prefix, Term]),
            Acc#{Key => to_bin(Value)}
        end,
        Labels1,
        List),
    Meta2 = Meta#{labels => Labels2},
    Actor#{metadata:=Meta2}.


%% @doc
add_label(Key, Value, #{metadata:=Meta}=Actor) ->
    Labels1 = maps:get(labels, Meta, #{}),
    Labels2 = Labels1#{to_bin(Key) => Value},
    Meta2 = Meta#{labels => Labels2},
    Actor#{metadata:=Meta2}.



%% @private Generic parse with standard errors
-spec parse(map(), nklib_syntax:syntax()) ->
    {ok, map()} | nklib_syntax:error().

parse(Data, Syntax) ->
    parse(Data, Syntax, #{}).


%% @private Generic parse with standard errors
-spec parse(map(), nklib_syntax:syntax(), nklib_syntax:parse_opts()) ->
    {ok, map()} | nklib_syntax:error().

parse(Data, Syntax, Opts) ->
    %lager:error("NKLOG SYN Data:~p\n Syntax:~p", [Data, Syntax]),
    case nklib_syntax:parse(Data, Syntax, Opts) of
        {ok, Data2, []} ->
            {ok, Data2};
        {ok, _, [Field | _]} ->
            {error, {field_unknown, Field}};
        {error, {syntax_error, Field}} ->
            % lager:error("NKLOG Data ~p Syntax ~p", [Data, Syntax]),
            {error, {field_invalid, Field}};
        {error, {field_missing, Field}} ->
            {error, {field_missing, Field}};
        {error, Error} ->
            lager:error("Unexpected parse error at ~p: ~p", [?MODULE, Error]),
            {error, Error}
    end.

%% @private
-spec parse_actor_data(actor(), nklib_syntax:syntax()) ->
    {ok, actor()} | nklib_syntax:error().

parse_actor_data(#{data:=Data}=Actor, Syntax) ->
    case parse(Data, Syntax, #{path=><<"data">>}) of
        {ok, Data2} ->
            {ok, Actor#{data:=Data2}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
-spec parse_request_params(nkactor:request(), nklib_syntax:syntax()) ->
    {ok, map()} | nklib_syntax:error().

parse_request_params(Req, Syntax) ->
    Params = maps:get(params, Req, #{}),
    case nklib_syntax:parse(Params, Syntax) of
        {ok, Data2, []} ->
            {ok, Data2};
        {error, {syntax_error, Field}} ->
            {error, {parameter_invalid, Field}};
        {error, {field_missing, Field}} ->
            {error, {parameter_missing, Field}};
        {error, Error} ->
            lager:error("Unexpected parse error at ~p: ~p", [?MODULE, Error]),
            {error, Error}
    end.


%% @doc Sends an out-of-actor event
-spec send_external_event(nkactor:id(), created|deleted|updated, actor()) ->
    ok.

send_external_event(SrvId, Reason, Actor) ->
    ?CALL_SRV(SrvId, actor_external_event, [SrvId, Reason, Actor]).


%% @doc
check_links(#{metadata:=Meta1}=Actor) ->
    case do_check_links(Meta1) of
        {ok, Meta2} ->
            {ok, Actor#{metadata:=Meta2}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
do_check_links(#{links:=Links}=Meta) ->
    case do_check_links(maps:to_list(Links), []) of
        {ok, Links2} ->
            {ok, Meta#{links:=Links2}};
        {error, Error} ->
            {error, Error}
    end;

do_check_links(Meta) ->
    {ok, Meta}.


%% @private
do_check_links([], Acc) ->
    {ok, maps:from_list(Acc)};

do_check_links([{Id, Type}|Rest], Acc) ->
    case nkactor:find(Id) of
        {ok, #actor_id{uid=UID}} ->
            true = is_binary(UID),
            do_check_links(Rest, [{UID, Type}|Acc]);
        {error, actor_not_found} ->
            lager:error("NKLOG LINKED NOT FOUND ~p", [Id]),
            {error, linked_actor_unknown};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Prepares an actor for creation
%% - uid is added
%% - name is added (if not present)
%% - metas creationTime, updateTime, generation and resourceVersion are added
add_creation_fields(Actor) ->
    Res = maps:get(resource, Actor),
    Meta = maps:get(metadata, Actor, #{}),
    UID = make_uid(Res),
    Name1 = maps:get(name, Actor, <<>>),
    %% Add Name if not present
    Name2 = case normalized_name(to_bin(Name1)) of
        <<>> ->
            make_name(UID);
        NormName ->
            NormName
    end,
    Time = nklib_date:now_3339(usecs),
    Actor2 = Actor#{
        name => Name2,
        uid => UID,
        metadata => Meta#{creation_time => Time}
    },
    update(Actor2, Time).


%% @private
update(Actor, Time3339) ->
    #{
        group := Group,
        resource := Res,
        name := Name,
        namespace := Namespace,
        metadata := Meta,
        data := Data
    } = Actor,
    Gen = maps:get(generation, Meta, -1),
    Hash = erlang:phash2({Namespace, Group, Res, Name, Data, Meta}),
    Meta2 = Meta#{
        update_time => Time3339,
        generation => Gen+1,
        hash => to_bin(Hash)
    },
    Actor#{metadata := Meta2}.



%% @private
make_rev_path(Namespace) ->
    Parts = make_rev_parts(Namespace),
    nklib_util:bjoin(Parts, $.).


%% @private
make_rev_parts(Namespace) ->
    case to_bin(Namespace) of
        <<>> ->
            [];
        Namespace2 ->
            lists:reverse(binary:split(Namespace2, <<$.>>, [global]))
    end.


%% @private
make_uid(Res) ->
    UUID = nklib_util:luid(),<<(to_bin(Res))/binary, $-, UUID/binary>>.


%% @private
make_name(Id) ->
    UUID = case binary:split(Id, <<"-">>) of
        [_, Rest] when byte_size(Rest) >= 7 ->
            Rest;
        [Rest] when byte_size(Rest) >= 7 ->
            Rest;
        _ ->
            nklib_util:luid()
    end,
    normalized_name(binary:part(UUID, 0, 12)).


%% @private
normalized_name(Name) ->
    nklib_parse:normalize(Name, #{space=>$_, allowed=>[$+, $-, $., $_]}).


%% @private
make_plural(Type) ->
    Type2 = to_bin(Type),
    Size = byte_size(Type2),
    case binary:at(Type2, Size-1) of
        $s ->
            <<Type2/binary, "es">>;
        $y ->
            <<Type2:(Size-1)/binary, "ies">>;
        _ ->
            <<Type2/binary, "s">>
    end.


%% @private
make_singular(Resource) ->
    Word = case lists:reverse(nklib_util:to_list(Resource)) of
        [$s, $e, $i|Rest] ->
            [$y|Rest];
        [$s, $e, $s|Rest] ->
            [$s|Rest];
        [$s|Rest] ->
            Rest;
        Rest ->
            Rest
    end,
    list_to_binary(lists:reverse(Word)).


%% @private
fts_normalize_word(Word) ->
    nklib_parse:normalize(Word, #{unrecognized=>keep}).


%% @doc
fts_normalize_multi(Text) ->
    nklib_parse:normalize_words(Text, #{unrecognized=>keep}).


%% @doc
update_check_fields(NewActor, #actor_st{actor=OldActor, config=Config}) ->
    #{data:=NewData} = NewActor,
    #{data:=OldData} = OldActor,
    Fields = maps:get(immutable_fields, Config, []),
    do_update_check_fields(Fields, NewData, OldData).


%% @private
do_update_check_fields([], _NewData, _OldData) ->
    ok;

do_update_check_fields([Field|Rest], NewData, OldData) ->
    case binary:split(Field, <<".">>) of
        [Group, Key] ->
            SubNew = maps:get(Group, NewData, #{}),
            SubOld = maps:get(Group, OldData, #{}),
            case do_update_check_fields([Key], SubNew, SubOld) of
                ok ->
                    do_update_check_fields(Rest, NewData, OldData);
                {error, {updated_invalid_field, _}} ->
                    {error, {updated_invalid_field, Field}}
            end;
        [_] ->
            case maps:find(Field, NewData) == maps:find(Field, OldData) of
                true ->
                    do_update_check_fields(Rest, NewData, OldData);
                false ->
                    {error, {updated_invalid_field, Field}}
            end
    end.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).
