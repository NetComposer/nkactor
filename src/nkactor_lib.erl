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

-export([actor_to_actor_id/1, id_to_actor_id/1]).
-export([send_external_event/3]).
-export([get_linked_type/2, get_linked_uids/2, add_link/3, add_checked_link/4, add_checked_link/5,
         rm_link/2, rm_links/3, rm_links/2, link_type/2]).
-export([add_creation_fields/2, update/2, check_actor_links/1, check_meta_links/1, check_links/1]).
-export([add_labels/4, add_label/3, rm_label_re/2]).
-export([actor_id_to_path/1]).
-export([parse/2, parse/3, parse_actor_data/3, parse_request_params/2]).
-export([make_rev_path/1, make_rev_parts/1]).
-export([make_plural/1, make_singular/1, normalized_name/1]).
-export([add_fts/3, fts_normalize_word/1, fts_normalize_multi/1]).
-export([update_check_fields/2]).
-export([maybe_set_ttl/2, set_ttl/2]).
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
    case Actor of
        #{
            group := Group,
            resource := Resource,
            name := Name,
            namespace := Namespace
        } ->
            #actor_id{
                group = to_bin(Group),
                resource = to_bin(Resource),
                name = to_bin(Name),
                namespace = to_bin(Namespace),
                uid = maps:get(uid, Actor, undefined),
                pid = undefined
            };
        #{
            uid := UID
        } ->
            #actor_id{uid = UID}
    end.



%% @doc Canonizes id to #actor_id{}
%% Id can be:
%% - If it starts with "/" it is assumed external and called actor_id/2 on guessed service
%%   (if a 'namespaces' part in the url is found, otherwhise any service)
%% - Group:Resource:Name.Namespace
%% - Resource:Name.Namespace (group will be undefined, some backends will not support it)
%% - Name.Namespace (group and resource will be undefined, some backends will not support it)
%% - Otherwise, it is assumed to be an UID (group, resource name and namespace will be undefined)
-spec id_to_actor_id(nkactor:id()) ->
    #actor_id{}.

id_to_actor_id(#actor_id{}=ActorId) ->
    ActorId;

id_to_actor_id(Path) ->
    Path2 = to_bin(Path),
    case Path2 of
        <<"/", Path3/binary>> ->
            Parts = binary:split(Path3, <<"/">>, [global]),
            case find_namespace_in_parts(Parts) of
                {ok, Namespace} ->
                    case nkactor_namespace:find_service(Namespace) of
                        {ok, SrvId} ->
                            case ?CALL_SRV(SrvId, actor_id, [SrvId, Parts]) of
                                #actor_id{} = ActorId ->
                                    ActorId;
                                continue ->
                                    #actor_id{uid=Path2}
                            end;
                        _ ->
                            lager:notice("NkACTOR id_to_actor_id unknown namespace: ~s", [Path2]),
                            case nkactor:call_services(actor_id, [Parts]) of
                                #actor_id{} = ActorId ->
                                    ActorId;
                                continue ->
                                    #actor_id{uid=Path2}
                            end
                    end;
                error ->
                    #actor_id{uid=Path2}
            end;
        _ ->
            case binary:split(Path2, <<$.>>) of
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


%% @private
find_namespace_in_parts([<<"namespaces">>, Namespace | _]) ->
    {ok, Namespace};

find_namespace_in_parts([_|Rest]) ->
    find_namespace_in_parts(Rest);

find_namespace_in_parts([]) ->
    error.

%% @doc
add_link(#actor_id{uid=UID}, #{}=Actor, LinkType) when is_binary(LinkType) ->
    add_link(UID, Actor, LinkType);

add_link(UID, #{metadata:=Meta}=Actor, LinkType) when is_binary(UID), is_binary(LinkType) ->
    Links1 = maps:get(links, Meta, #{}),
    Links2 = Links1#{UID => LinkType},
    Actor#{metadata:=Meta#{links => Links2}}.


%% @doc Links checking destination type, generates type
add_checked_link(Target, Group, Resource, #{}=Actor) when is_binary(Group), is_binary(Resource) ->
    LinkType = link_type(Group, Resource),
    add_checked_link(Target, Group, Resource, Actor, LinkType).


%% @doc Links checking destination type
add_checked_link(Target, Group, Resource, #{}=Actor, LinkType)
        when is_binary(Group), is_binary(Resource), is_binary(LinkType) ->
    case nkactor:find(Target) of
        {ok, #actor_id{group=Group, resource=Resource, uid=UID}=TargetId} ->
            #{metadata:=Meta} = Actor,
            Links1 = maps:get(links, Meta, #{}),
            Links2 = Links1#{UID => LinkType},
            {ok, TargetId, Actor#{metadata:=Meta#{links => Links2}}};
        _ ->
            {error, {linked_actor_unknown, Target}}
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
    <<"io.netk.", Group/binary, $., Resource/binary>>.


%% @doc Removes a link
rm_link(#{metadata:=Meta}=Actor, Target) when is_binary(Target) ->
    Links1 = maps:get(links, Meta, #{}),
    Links2 = maps:remove(Target, Links1),
    Actor#{metadata:=Meta#{links=>Links2}}.


%% @doc Removes all links for a type
rm_links(Group, Resource, #{}=Actor) ->
    LinkType = link_type(Group, Resource),
    rm_links(Actor, LinkType).


%% @doc Removes all links for a type
rm_links(#{metadata:=Meta}=Actor, LinkType) when is_binary(LinkType)->
    Links = maps:get(links, Meta, #{}),
    case maps:filter(fun(_UID, LT) -> LT /= LinkType end, Links) of
        Links ->
            Actor;
        Links2 ->
            Actor#{metadata:=Meta#{links=>Links2}}
    end.


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


%% @doc Remove all labels with a pattern
rm_label_re(Bin, #{metadata:=Meta}=Actor) ->
    Labels = maps:filter(
        fun(Key, _V) -> binary:match(Key, Bin)==nomatch end,
        maps:get(labels, Meta, #{})),
    Actor#{metadata:=Meta#{labels => Labels}}.


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
-spec parse_actor_data(actor(), nkactor:vsn(), nklib_syntax:syntax()) ->
    {ok, actor()} | nklib_syntax:error().

parse_actor_data(#{data:=Data, metadata:=Meta}=Actor, Vsn, Syntax) ->
    case parse(Data, Syntax, #{path=><<"data">>}) of
        {ok, Data2} ->
            {ok, Actor#{data:=Data2, metadata:=Meta#{vsn=>Vsn}}};
        {error, Error} ->
            {error, Error}
    end.



%% @private
-spec parse_request_params(nkactor_request:request(), nklib_syntax:syntax()) ->
    {ok, map()} | nklib_syntax:error().

parse_request_params(Req, Syntax) ->
    Params = maps:get(params, Req, #{}),
    case nklib_syntax:parse_all(Params, Syntax) of
        {ok, Data2} ->
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
check_actor_links(#{metadata:=Meta1}=Actor) ->
    case check_meta_links(Meta1) of
        {ok, Meta2} ->
            {ok, Actor#{metadata:=Meta2}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
check_meta_links(#{links:=Links}=Meta) ->
    case check_links(Links) of
        {ok, Links2} ->
            {ok, Meta#{links:=Links2}};
        {error, Error} ->
            {error, Error}
    end;

check_meta_links(Meta) ->
    {ok, Meta}.


%% @private
check_links(Links) ->
    do_check_links(maps:to_list(Links), []).


%% @private
%% Checks if links exists, changes to UID. If empty value, it is removed
do_check_links([], Acc) ->
    {ok, maps:from_list(Acc)};

do_check_links([{_Id, <<>>}|Rest], Acc) ->
    do_check_links(Rest, Acc);

do_check_links([{Id, Type}|Rest], Acc) ->
    case nkactor:find(Id) of
        {ok, #actor_id{uid=UID}} ->
            true = is_binary(UID),
            do_check_links(Rest, [{UID, Type}|Acc]);
        {error, actor_not_found} ->
            {error, {linked_actor_unknown, Id}};
        {error, Error} ->
            {error, Error}
    end.




%% @doc Prepares an actor for creation
%% - uid is added
%% - name is added (if not present)
%% - metas kind, creationTime, updateTime, generation and resourceVersion are added
add_creation_fields(SrvId, Actor) ->
    Group = maps:get(group, Actor),
    Res = maps:get(resource, Actor),
    Module = nkactor_actor:get_module(SrvId, Group, Res),
    #{camel:=Camel} = nkactor_actor:get_config(SrvId, Module),
    Meta = maps:get(metadata, Actor, #{}),
    UID = make_uid(Res),
    Name1 = maps:get(name, Actor, <<>>),
    %% Add Name if not present
    Name2 = case normalized_name(to_bin(Name1)) of
        <<>> ->
            make_name(UID);
        <<"undefined">> ->
            make_name(UID);
        NormName ->
            NormName
    end,
    Time = nklib_date:now_3339(usecs),
    Actor2 = Actor#{
        name => Name2,
        uid => UID,
        metadata => Meta#{
            kind => Camel,
            creation_time => Time
        }
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
    Time = nklib_date:now_hex(msecs),   % 12 bytes
    <<UUID:15/binary, _/binary>> = nklib_util:luid(),
    <<(to_bin(Res))/binary, $-, Time/binary, UUID/binary>>.


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
    nklib_parse:normalize(Name, ?NORMALIZE_OPTS).


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
add_fts(Field, Value, #{metadata:=Meta}=Actor) ->
    Fts1 = maps:get(fts, Meta, #{}),
    Fts2 = Fts1#{to_bin(Field) => to_bin(Value)},
    Actor#{metadata:=Meta#{fts => Fts2}}.



%% @private
fts_normalize_word(Word) ->
    nklib_parse:normalize(Word, #{unrecognized=>keep}).


%% @doc
fts_normalize_multi(Text) ->
    nklib_parse:normalize_words(Text, #{unrecognized=>keep}).


%% @doc
update_check_fields(NewActor, ActorSt) ->
    #{data:=NewData} = NewActor,
    #actor_st{actor=OldActor, config=Config} = ActorSt,
    #{data:=OldData} = OldActor,
    Fields1 = maps:get(fields_static, Config, []),
    Fields2 = [F || <<"data.", F/binary>> <- Fields1],
    do_update_check_fields(Fields2, NewData, OldData).


%% @private
do_update_check_fields([], _NewData, _OldData) ->
    ok;

do_update_check_fields([Field|Rest], NewData, OldData) ->
    case binary:split(Field, <<".">>) of
        [Group1, Key] ->
            Group2 = binary_to_existing_atom(Group1, utf8),
            SubNew = maps:get(Group2, NewData, #{}),
            SubOld = maps:get(Group2, OldData, #{}),
            case do_update_check_fields([Key], SubNew, SubOld) of
                ok ->
                    do_update_check_fields(Rest, NewData, OldData);
                {error, {updated_invalid_field, _}} ->
                    {error, {updated_invalid_field, <<"data.", Field/binary>>}}
            end;
        [_] ->
            Field2 = binary_to_existing_atom(Field, utf8),
            case maps:find(Field2, NewData) == maps:find(Field2, OldData) of
                true ->
                    do_update_check_fields(Rest, NewData, OldData);
                false ->
                    {error, {updated_invalid_field, <<"data.", Field/binary>>}}
            end
    end.



%% @private
%% if expires_time is not set, it is set to now + indicated time
maybe_set_ttl(#{metadata:=#{expires_time:=_}}=Actor, _MSecs) ->
    Actor;

maybe_set_ttl(Actor, MSecs) ->
    set_ttl(Actor, MSecs).


%% @private
%% sets expires_time to now + indicated time
set_ttl(#{metadata:=Meta}=Actor, MSecs) ->
    Now = nklib_date:epoch(usecs),
    {ok, Expires} = nklib_date:to_3339(Now+1000*MSecs, usecs),
    Meta2 = Meta#{expires_time => Expires},
    Actor#{metadata := Meta2}.


%% @private
to_bin(T) when is_binary(T)-> T;
to_bin(T) -> nklib_util:to_binary(T).
