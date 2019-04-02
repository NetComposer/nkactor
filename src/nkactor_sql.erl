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


%% @doc SQL utilities for stores to use
-module(nkactor_sql).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([select/2, filters/2, sort/2]).
-export([quote/1, filter_path/2]).

-include_lib("nkactor/include/nkactor.hrl").


%% ===================================================================
%% Select
%% ===================================================================


%% @private
select(#{only_uid:=true}, Table) ->
    [<<"SELECT uid FROM ">>, to_bin(Table)];

select(Params, Table) ->
    [
        <<"SELECT uid,namespace,\"group\",resource,name">>,
        case maps:get(get_metadata, Params, false) of
            true ->
                <<",metadata">>;
            false ->
                []
            end,
        case maps:get(get_data, Params, false) of
            true ->
                <<",data">>;
            false ->
                []
        end,
        <<" FROM ">>, to_bin(Table)
    ].


%% ===================================================================
%% Filters
%% ===================================================================


%% @private
filters(#{namespace:=Namespace}=Params, Table) ->
    Flavor = maps:get(flavor, Params, #{}),
    Filters = maps:get(filter, Params, #{}),
    AndFilters1 = expand_filter(maps:get('and', Filters, []), []),
    AndFilters2 = make_filter(AndFilters1, Table, Flavor, []),
    OrFilters1 = expand_filter(maps:get('or', Filters, []), []),
    OrFilters2 = make_filter(OrFilters1, Table, Flavor, []),
    OrFilters3 = nklib_util:bjoin(OrFilters2, <<" OR ">>),
    OrFilters4 = case OrFilters3 of
        <<>> ->
            [];
        _ ->
            [<<$(, OrFilters3/binary, $)>>]
    end,
    NotFilters1 = expand_filter(maps:get('not', Filters, []), []),
    NotFilters2 = make_filter(NotFilters1, Table, Flavor, []),
    NotFilters3 = case NotFilters2 of
        <<>> ->
            [];
        _ ->
            [<<"(NOT ", F/binary, ")">> || F <- NotFilters2]
    end,
    Deep = maps:get(deep, Params, false),
    PathFilter = list_to_binary(filter_path(Namespace, Deep)),
    FilterList = [PathFilter | AndFilters2 ++ OrFilters4 ++ NotFilters3],
    Where = nklib_util:bjoin(FilterList, <<" AND ">>),
    [<<" WHERE ">>, Where].


%% @private
expand_filter([], Acc) ->
    Acc;

expand_filter([#{field:=Field, value:=Value}=Term|Rest], Acc) ->
    Op = maps:get(op, Term, eq),
    Type = maps:get(type, Term, string),
    Value2 = case Type of
        _ when Op==exists ->
            to_boolean(Value);
        string when Op==values, is_list(Value) ->
            [to_bin(V) || V <- Value];
        string ->
            to_bin(Value);
        integer when Op==values, is_list(Value) ->
            [to_integer(V) || V <- Value];
        integer ->
            to_integer(Value);
        boolean when Op==values, is_list(Value) ->
            [to_boolean(V) || V <- Value];
        boolean ->
            to_boolean(Value);
        object ->
            Value
    end,
    expand_filter(Rest, [{Field, Op, Value2, Type}|Acc]).



%% @private
make_filter([], _Table, _Flavor, Acc) ->
    Acc;

make_filter([{<<"group+resource">>, eq, Val, string} | Rest], actors, Flavor, Acc) ->
    [Group, Type] = binary:split(Val, <<"+">>),
    Filter = <<"(\"group\"='", Group/binary, "' AND resource='", Type/binary, "')">>,
    make_filter(Rest, actors, Flavor, [Filter | Acc]);

make_filter([{<<"metadata.fts.", Field/binary>>, Op, Val, string} | Rest], actors, Flavor, Acc) ->
    Word = nkactor_lib:fts_normalize_word(Val),
    Filter = case {Field, Op} of
        {<<"*">>, eq} ->
            % We search for an specific word in all fields
            % For example fieldX:valueY, found by LIKE '%:valueY %'
            % (final space makes sure word has finished)
            <<"(fts_words LIKE '%:", Word/binary, " %')">>;
        {<<"*">>, prefix} ->
            % We search for a prefix word in all fields
            % For example fieldX:valueYXX, found by LIKE '%:valueY%'
            <<"(fts_words LIKE '%:", Word/binary, "%')">>;
        {_, eq} ->
            % We search for an specific word in an specific fields
            % For example fieldX:valueYXX, found by LIKE '% FieldX:valueY %'
            <<"(fts_words LIKE '% ", Field/binary, $:, Word/binary, " %')">>;
        {_, prefix} ->
            % We search for a prefix word in an specific fields
            % For example fieldX:valueYXX, found by LIKE '% FieldX:valueY%'
            <<"(fts_words LIKE '% ", Field/binary, $:, Word/binary, "%')">>;
        _ ->
            <<"(TRUE = FALSE)">>
    end,
    make_filter(Rest, actors, Flavor, [Filter | Acc]);

make_filter([{<<"metadata.is_enabled">>, eq, true, boolean}|Rest], actors, Flavor, Acc) ->
    Filter = <<"(NOT metadata @> '{\"is_enabled\":false}')">>,
    make_filter(Rest, actors, Flavor, [Filter|Acc]);

make_filter([{<<"metadata.is_enabled">>, eq, false, boolean}|Rest], actors, Flavor, Acc) ->
    Filter = <<"(metadata @> '{\"is_enabled\":false}')">>,
    make_filter(Rest, actors, Flavor, [Filter|Acc]);

% The @> operator is the only one that uses the inverted index
% This group of queries will work for any type, and also for 'object'
make_filter([{<<"data.", Field/binary>>, eq, Value, _}|Rest], actors, Flavor, Acc) ->
    Json = make_json_spec(Field, Value),
    Filter = [<<"(data @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"data.", Field/binary>>, exists, Bool, _}|Rest], actors, Flavor, Acc) ->
    Not = case Bool of true -> []; false -> <<"NOT ">> end,
    Filter = [<<"(">>, Not, <<"data @> '">>, make_json_spec(Field), <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

% Do it explicitly to allow '.' in label keys
make_filter([{<<"metadata.labels.", Label/binary>>, eq, Value, _}|Rest], actors, Flavor, Acc) ->
    Json = nklib_json:encode(#{<<"labels">>=>#{Label=>Value}}),
    Filter = [<<"(metadata @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.labels.", Label/binary>>, exists, Bool, _}|Rest], actors, Flavor, Acc) ->
    Not = case Bool of true -> []; false -> <<"NOT ">> end,
    Json = nklib_json:encode(#{<<"labels">>=>Label}),
    Filter = [<<"(">>, Not, <<"metadata @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

% Label table!
make_filter([{<<"metadata.labels.", Label/binary>>, exists, Bool, _}|Rest], labels, Flavor, Acc) ->
    Not = case Bool of true -> []; false -> <<"NOT ">> end,
    Filter = [<<"(">>, Not, <<"label_key = ">>, quote(Label), <<")">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.labels.", Label/binary>>, Op, Value, _}|Rest], labels, Flavor, Acc) ->
    Filter = [
        <<"(label_key = ">>, quote(Label), <<") AND ">>,
        <<"(">>, get_op(<<"label_value">>, Op, Value), <<")">>
    ],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.", Field/binary>>, eq, Value, _}|Rest], actors, Flavor, Acc) ->
    Json = make_json_spec(Field, Value),
    Filter = [<<"(metadata @> '">>, Json, <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{<<"metadata.", Field/binary>>, exists, Bool, _}|Rest], actors, Flavor, Acc) ->
    Not = case Bool of true -> []; false -> <<"NOT ">> end,
    Filter = [<<"(">>, Not, <<"metadata @> '">>, make_json_spec(Field), <<"')">>],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]);

make_filter([{Field, _Op, _Val, object} | Rest], actors, Flavor, Acc) ->
    lager:warning("using invalid object operator at ~p: ~p", [?MODULE, Field]),
    make_filter(Rest, actors, Flavor, Acc);

make_filter([{Field, exists, Bool, _}|Rest], actors, Flavor, Acc)
    when Field==<<"uid">>; Field==<<"namespace">>; Field==<<"group">>;
    Field==<<"resource">>; Field==<<"path">>; Field==<<"hash">>; Field==<<"last_update">>;
    Field==<<"expires">>; Field==<<"fts_word">> ->
    Acc2 = case Bool of
        true ->
            Acc;
        false ->
            % Force no records
            [<<"(TRUE = FALSE)">>|Acc]
    end,
    make_filter(Rest, actors, Flavor, Acc2);

make_filter([{Field, exists, Bool, _}|Rest], actors, Flavor, Acc) ->
    Field2 = get_field_db_name(Field),
    L = binary:split(Field2, <<".">>, [global]),
    [Field3|Base1] = lists:reverse(L),
    Base2 = nklib_util:bjoin(lists:reverse(Base1), $.),
    Filter = [
        case Bool of
            true -> <<"(">>;
            false -> <<"(NOT ">>
        end,
        json_value(Base2, json),
        <<" ? ">>, quote(Field3), <<")">>
    ],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, prefix, Val, string}|Rest], actors, Flavor, Acc) ->
    Field2 = get_field_db_name(Field),
    Filter = [
        $(,
        json_value(Field2, string),
        <<" LIKE ">>, quote(<<Val/binary, $%>>), $)
    ],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, ValList, Type}|Rest], actors, Flavor, Acc) when is_list(ValList) ->
    Values = nklib_util:bjoin([quote(Val) || Val <- ValList], $,),
    Field2 = get_field_db_name(Field),
    Filter = [
        $(,
        json_value(Field2, Type),
        <<" IN (">>, Values, <<"))">>
    ],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter)|Acc]);

make_filter([{Field, values, Value, Type}|Rest], actors, Flavor, Acc) ->
    make_filter([{Field, values, [Value], Type}|Rest], actors, Flavor, Acc);

make_filter([{Field, Op, Val, Type} | Rest], actors, Flavor, Acc) ->
    Field2 = get_field_db_name(Field),
    Filter = [$(, get_op(json_value(Field2, Type), Op, Val), $)],
    make_filter(Rest, actors, Flavor, [list_to_binary(Filter) | Acc]).


%% @private
get_op(Field, eq, Value) -> [Field, << "=" >>, quote(Value)];
get_op(Field, ne, Value) -> [Field, <<" <> ">>, quote(Value)];
get_op(Field, lt, Value) -> [Field, <<" < ">>, quote(Value)];
get_op(Field, lte, Value) -> [Field, <<" <= ">>, quote(Value)];
get_op(Field, gt, Value) -> [Field, <<" > ">>, quote(Value)];
get_op(Field, gte, Value) -> [Field, <<" >= ">>, quote(Value)];
get_op(_Field, exists, _Value) -> [<<"TRUE">>].


%% @private
get_field_db_name(<<"uid">>) -> <<"uid">>;
get_field_db_name(<<"namespace">>) -> <<"namespace">>;
get_field_db_name(<<"group">>) -> <<"\"group\"">>;
get_field_db_name(<<"resource">>) -> <<"resource">>;
get_field_db_name(<<"name">>) -> <<"name">>;
get_field_db_name(<<"hash">>) -> <<"hash">>;
get_field_db_name(<<"path">>) -> <<"path">>;
get_field_db_name(<<"last_update">>) -> <<"last_update">>;
get_field_db_name(<<"expires">>) -> <<"expires">>;
get_field_db_name(<<"fts_word">>) -> <<"fts_word">>;
get_field_db_name(<<"data.", _/binary>>=Field) -> Field;
get_field_db_name(<<"metadata.hash">>) -> <<"hash">>;
get_field_db_name(<<"metadata.update_time">>) -> <<"last_update">>;
% Any other metadata is kept
get_field_db_name(<<"metadata.", _/binary>>=Field) -> Field;
% Any other field should be inside data in this implementation
get_field_db_name(Field) -> <<"data.", Field/binary>>.



%% ===================================================================
%% Sort
%% ===================================================================


%% @private
sort(Params, Table) ->
    Flavor = maps:get(flavor, Params, #{}),
    Sort = expand_sort(maps:get(sort, Params, []), []),
    make_sort(Sort, Table, Flavor, []).


%% @private
expand_sort([], Acc) ->
    lists:reverse(Acc);

expand_sort([#{field:=Field}=Term|Rest], Acc) ->
    case Field of
        <<"group+resource">> ->
            % Special field used in namespaces
            expand_sort([Term#{field:=<<"group">>}, Term#{field:=<<"resource">>}|Rest], Acc);
        _ ->
            Order = maps:get(order, Term, asc),
            Type = maps:get(type, Term, string),
            expand_sort(Rest, [{Order, Field, Type}|Acc])
    end.


%% @private
make_sort([], _Table, _Flavor, []) ->
    <<>>;

make_sort([], _Table, _Flavor, Acc) ->
    [<<" ORDER BY ">>, nklib_util:bjoin(lists:reverse(Acc), $,)];

make_sort([{Order, <<"metadata.labels", _/binary>>, _Type}|Rest], labels, Flavor, Acc) ->
    Item = [
        <<"label_key">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end,
        <<", label_value">>,
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, labels, Flavor, [list_to_binary(Item)|Acc]);

make_sort([{Order, Field, Type}|Rest], actors, Flavor, Acc) ->
    Item = [
        json_value(get_field_db_name(Field), Type),
        case Order of asc -> <<" ASC">>; desc -> <<" DESC">> end
    ],
    make_sort(Rest, actors, Flavor, [list_to_binary(Item)|Acc]).



%% ===================================================================
%% Utilities
%% ===================================================================



%% @private
%% Extracts a field inside a JSON,  it and casts it to json, string, integer o boolean
json_value(Field, Type) ->
    json_value(Field, Type, [], []).


%% @private
json_value(Field, Type, [links, <<"metadata">>], Acc) ->
    finish_json_value(Type, Field, Acc);

json_value(Field, Type, [labels, <<"metadata">>], Acc) ->
    finish_json_value(Type, Field, Acc);

json_value(Field, Type, Heads, Acc) ->
    case binary:split(Field, <<".">>) of
        [Single] when Acc==[] ->
            % No "." at all
            Single;
        [Last] ->
            finish_json_value(Type, Last, Acc);
        [Base, Rest] when Acc==[] ->
            json_value(Rest, Type, [Base|Heads], [Base, <<"->">>]);
        [Base, Rest] ->
            json_value(Rest, Type, [Base|Heads], Acc++[$', Base, $', <<"->">>])
    end.

finish_json_value(Type, Last, Acc) ->
    case Type of
        json ->
            Acc++[$', Last, $'];
        string ->
            Acc++[$>, $', Last, $'];    % '>' finishes ->>
        integer ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::INTEGER">>];
        boolean ->
            [$(|Acc] ++ [$>, $', Last, $', <<")::BOOLEAN">>]
    end.


%% @private Generates a JSON based on a field
%% make_json_spec(<<"a.b.c">>) = {"a":{"b":"c"}}
make_json_spec(Field) ->
    List = binary:split(Field, <<".">>, [global]),
    nklib_json:encode(make_json_spec2(List)).


%% @private Generates a JSON based on a field
%% make_json_spec(<<"a.b">>, 1) = {"a":{"b":1}}
make_json_spec(Field, Value) ->
    List = binary:split(Field, <<".">>, [global]) ++ [Value],
    nklib_json:encode(make_json_spec2(List)).


%% @private
make_json_spec2([]) -> #{};
make_json_spec2([Last]) -> Last;
make_json_spec2([Field|Rest]) -> #{Field => make_json_spec2(Rest)}.



%% @private
quote(Field) when is_binary(Field) -> [$', to_field(Field), $'];
quote(Field) when is_list(Field) -> [$', to_field(Field), $'];
quote(Field) when is_integer(Field); is_float(Field) -> to_bin(Field);
quote(true) -> <<"TRUE">>;
quote(false) -> <<"FALSE">>;
quote(null) -> <<"NULL">>;
quote(Field) when is_atom(Field) -> quote(atom_to_binary(Field, utf8));
quote(Field) when is_map(Field) ->
    case nklib_json:encode(Field) of
        error ->
            lager:error("Error enconding JSON: ~p", [Field]),
            error(json_encode_error);
        Json ->
            [$', to_field(Json), $']
    end.


%% @private
to_field(Field) ->
    Field2 = to_bin(Field),
    case binary:match(Field2, <<$'>>) of
        nomatch ->
            Field2;
        _ ->
            re:replace(Field2, <<$'>>, <<$',$'>>, [global, {return, binary}])
    end.



%% @private
filter_path(<<>>, true) ->
    [<<"TRUE">>];

filter_path(Namespace, Deep) ->
    Path = nkactor_lib:make_rev_path(Namespace),
    case Deep of
        true ->
            [<<"(path LIKE ">>, quote(<<Path/binary, "%">>), <<")">>];
        false ->
            [<<"(path = ">>, quote(Path), <<")">>]
    end.




%% ===================================================================
%% Internal
%% ===================================================================


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).


%% @private
to_integer(Term) when is_integer(Term) ->
    Term;
to_integer(Term) ->
    case nklib_util:to_integer(Term) of
        error -> 0;
        Integer -> Integer
    end.

%% @private
to_boolean(Term) when is_boolean(Term) ->
    Term;
to_boolean(Term) ->
    case nklib_util:to_boolean(Term) of
        error -> false;
        Boolean -> Boolean
    end.
