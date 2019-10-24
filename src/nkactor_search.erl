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

%% @doc Actor Search
-module(nkactor_search).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-export([parse_spec/2]).
-export_type([spec/0, opts/0, filter/0, sort_spec/0]).

-include("nkactor.hrl").



%% ==================================================================
%% Types
%% ===================================================================


-type spec() ::
    #{
        namespace => nkactor:namespace(),
        deep => boolean(),
        from => pos_integer(),
        size => pos_integer(),
        get_total => boolean(),
        filter => filter(),
        sort => [sort_spec()],
        only_uid => boolean(),
        get_data => boolean(),
        get_metadata => boolean()
    }.


-type opts() ::
    #{
        forced_spec => spec(),              % Takes top priority
        default_spec => spec(),
        params => params(),                 % Take priority over spec
        default_sort => [sort_spec()],
        fields_filter => #{field_name() => true},
        fields_sort => #{field_name() => true},
        fields_type => #{field_name() => field_type()},
        fields_trans => #{field_name() => field_name()|fun((field_name()) -> field_name())}
    }.



-type filter() ::
    #{
        'and' => [filter_spec()],
        'or' => [filter_spec()],
        'not' => [filter_spec()]
    }.

% '.' used to separate levels in JSON
% Special fields would include:
% - label:'label'
% - linked:'uid'
% - fts:'



-type field_name() :: binary().


% Field types are used to cast the correct value from JSON
% By default, they will be extracted as strings (so they will be sorted incorrectly)
% For JSON fields, the type 'object' can be used for @> operator
% for example, for field spec.phone.phone => array, the query
%   #{field => "spec.phone.phone", eq=>"123} generates data->'spec'->'phone' @> '[{"phone": "123456"}]'
% For array, {"field":["value"]} will be used

-type field_type() :: string | boolean | integer | object | array.


-type filter_spec() ::
    #{
        field => field_name(),
        type => field_type(),
        op => filter_op(),
        value => value() | [value()]
    }.

-type filter_op() ::
    eq | ne | lt | lte | gt | gte | values | exists | prefix.


-type value() :: string() | binary() | integer() | boolean().


-type sort_spec() ::
    #{
        field => field_name(),      % '.' used to separate levels in JSON
        type => field_type(),
        order => asc | desc
    }.


-type params() ::
    #{
        namespace => binary(),
        from => pos_integer(),
        size => pos_integer(),
        sort => #{Field::binary() => asc|desc},
        labels => #{Name::binary() => Value::binary()},  %% Value == <<>> for 'any'
        fields => #{Name::binary() => Value::binary()},  %% Value can be "OP:Value"
        links => #{UID::binary() => Type::binary()},     %% Type == <<>> for 'any'
        fts => #{Field::binary() => Value::binary()},    %% Field == <<"*">> for 'any'
        deep => boolean(),
        get_total => boolean(),
        get_data => boolean(),
        get_metadata => boolean()
    }.


%% ===================================================================
%% Standard search
%% ===================================================================


%% @doc
%% If fields_filter is empty, anything is accepted
%% Same for fields_sort
%% If field is not in field_type, string is assumed
%% If a field is defined in FieldTypes, 'type' will be added
-spec parse_spec(spec(), opts()) ->
    {ok, spec()} | {error, term()}.

parse_spec(SearchSpec, Opts) ->
    %lager:error("NKLOG S1 ~p", [SearchSpec]),
    %lager:error("NKLOG S2 ~s", [nklib_json:encode_pretty(Opts)]),
    Syntax = search_spec_syntax(),
    case nklib_syntax:parse(SearchSpec, Syntax) of
        {ok, Parsed, []} ->
            case parse_params(Parsed, Opts) of
                {ok, Parsed2} ->
                    Parsed3 = apply_forced_spec(Parsed2, Opts),
                    Parsed4 = apply_default_spec(Parsed3, Opts),
                    case check_filters(Parsed4, Opts) of
                        {ok, Parsed5} ->
                            S = check_sort(Parsed5, Opts),
                            S;
                        {error, Error} ->
                            {error, Error}
                    end;
                {error, Error} ->
                    {error, Error}
            end;
            %{ok, analyze(Parsed)};
        {ok, _, [Field|_]} ->
            {error, {field_unknown, Field}};
        {error, Error} ->
            {error, Error}
    end.


%% @private
search_spec_syntax() ->
    #{
        from => pos_integer,
        size => pos_integer,
        namespace => binary,
        deep => boolean,
        get_total => boolean,
        filter => #{
            'and' => {list, search_spec_syntax_filter()},
            'or' => {list, search_spec_syntax_filter()},
            'not' => {list, search_spec_syntax_filter()}
        },
        sort => {list, search_spec_syntax_sort()},
        only_uid => boolean,
        get_data => boolean,
        get_metadata => boolean,
        ot_span_id => any,
        '__defaults' => #{namespace => <<>>}
    }.


%% @private
search_spec_syntax_filter() ->
    #{
        field => binary,
        type => {atom, [string, integer, boolean, object, array]},
        op => {atom, [eq, ne, lt, lte, gt, gte, values, exists, prefix, ignore]},
        value => any,
        '__mandatory' => [field, value]
    }.


%% @private
search_spec_syntax_sort() ->
    #{
        field => binary,
        type => {atom, [string, integer, boolean]},
        order => {atom, [asc, desc]},
        '__mandatory' => [field],
        '__defaults' => #{order => asc}
    }.


%% @private
apply_forced_spec(Spec, #{forced_spec:=Forced}) ->
    Spec2 = maps:merge(Spec, Forced),
    SpecFilter = maps:get(filter, Spec, #{}),
    ForcedFilter = maps:get(filter, Forced, #{}),
    And1 = maps:get('and', SpecFilter, []),
    And2 = maps:get('and', ForcedFilter, []),
    Filter2 = case And1++And2 of
        And1 ->
            SpecFilter;
        And3 ->
            SpecFilter#{'and' => And3}
    end,
    Or1 = maps:get('or', SpecFilter, []),
    Or2 = maps:get('or', ForcedFilter, []),
    Filter3 = case Or1++Or2 of
        Or1 ->
            Filter2;
        Or3 ->
            Filter2#{'or' => Or3}
    end,
    Not1 = maps:get('not', SpecFilter, []),
    Not2 = maps:get('not', ForcedFilter, []),
    Filter4 = case Not1++Not2 of
        Not1 ->
            Filter3;
        Not3 ->
            Filter3#{'or' => Not3}
    end,
    Spec3 = Spec2#{filter=>Filter4},
    Sort1 = maps:get(sort, Spec, []),
    Sort2 = maps:get(sort, Forced, []),
    Spec4 = case Sort2++Sort1 of
        Sort1 ->
            Spec3;
        Sort3 ->
            Spec3#{sort => Sort3}
    end,
    Spec4;

apply_forced_spec(Spec, _Opts) ->
    Spec.


%% @private
apply_default_spec(Spec, #{default_spec:=Default}) ->
    Spec2 = maps:merge(Default, Spec),
    Spec2;

apply_default_spec(Spec, _Opts) ->
    Spec.

%% @private
check_filters(#{filter:=Filter}=Spec, Opts) ->
    case check_filters(['and', 'or', 'not'], Filter, Opts) of
        {ok, Filter2} ->
            {ok, Spec#{filter:=Filter2}};
        {error, Error} ->
            {error, Error}
    end;

check_filters(Spec, _Opts) ->
    {ok, Spec}.


%% @private
check_filters([], Filter, _Opts) ->
    {ok, Filter};

check_filters([Type|Rest], Filter, Opts) ->
    case maps:find(Type, Filter) of
        {ok, FilterList} ->
            case check_field_filter(FilterList, Opts, []) of
                {ok, FilterList2} ->
                    check_filters(Rest, Filter#{Type:=FilterList2}, Opts);
                {error, Error} ->
                    {error, Error}
            end;
        error ->
            check_filters(Rest, Filter, Opts)
    end.


%% @private
%% Checks the filter in the valid list
check_field_filter([], _Opts, Acc) ->
    {ok, lists:reverse(Acc)};

check_field_filter([Filter|Rest], Opts, Acc) ->
    {Field2, Filter2} = field_trans(Filter, Opts),
    Filter3 = filter_op(Filter2),
    case Field2 of
        <<"metadata.labels.", _/binary>> ->
            check_field_filter(Rest, Opts, [Filter3|Acc]);
        <<"metadata.links.", _/binary>> ->
            check_field_filter(Rest, Opts, [Filter3|Acc]);
        <<"metadata.fts.", _/binary>> ->
            check_field_filter(Rest, Opts, [Filter3|Acc]);
        _ ->
            case maps:get(fields_filter, Opts, #{}) of
                Valid when map_size(Valid)==0 ->
                    case field_type(Field2, Filter3, Opts) of
                        {ok, Filter4} ->
                            check_field_filter(Rest, Opts, [Filter4|Acc]);
                        {error, Error} ->
                            {error, Error}
                    end;
                Valid ->
                    case maps:is_key(Field2, Valid) of
                        true ->
                            case field_type(Field2, Filter3, Opts) of
                                {ok, Filter4} ->
                                    check_field_filter(Rest, Opts, [Filter4|Acc]);
                                {error, Error} ->
                                    {error, Error}
                            end;
                        false ->
                            {error, {field_invalid, Field2}}
                    end
            end
    end.


%% @private
check_sort(#{sort:=Sort}=Spec, Opts) ->
    case check_field_sort(Sort, Opts, []) of
        {ok, Sort2} ->
            {ok, Spec#{sort:=Sort2}};
        {error, Error} ->
            {error, Error}
    end;

check_sort(Spec, _Opts) ->
    {ok, Spec}.


%% @private
%% Checks the filter in the valid list
check_field_sort([], _Opts, Acc) ->
    {ok, lists:reverse(Acc)};

check_field_sort([Sort|Rest], Opts, Acc) ->
    {Field2, Sort2} = field_trans(Sort, Opts),
    case maps:get(fields_sort, Opts, #{}) of
        Valid when map_size(Valid)==0 ->
            case field_type(Field2, Sort2, Opts) of
                {ok, Sort3} ->
                    check_field_sort(Rest, Opts, [Sort3|Acc]);
                {error, Error} ->
                    {error, Error}
            end;
        Valid ->
            case maps:is_key(Field2, Valid) of
                true ->
                    case field_type(Field2, Sort2, Opts) of
                        {ok, Sort3} ->
                            check_field_sort(Rest, Opts, [Sort3|Acc]);
                        {error, Error} ->
                            {error, Error}
                    end;
                false ->
                    {error, {field_invalid, Field2}}
            end
    end.


%% @private
field_trans(#{field:=Field}=Filter, #{fields_trans:=Trans}) ->
    Field2 = to_bin(Field),
    case maps:get(Field2, Trans, Field2) of
        Field ->
            {Field, Filter};
        Field3 ->
            {Field3, Filter#{field:=Field3}}
    end;

field_trans(#{field:=Field}=Filter, _) ->
    case to_bin(Field) of
        Field ->
            {Field, Filter};
        Field2 ->
            {Field2, Filter#{field:=Field2}}
    end.


%% @private
field_type(Field, Filter, Opts) ->
    Types = maps:get(fields_type, Opts, #{}),
    case maps:find(type, Filter) of
        {ok, UserType} ->
            case maps:get(Field, Types, UserType) of
                UserType ->
                    {ok, Filter#{field=>Field}};
                _ ->
                    {error, {conflicting_field_type, Field}}
            end;
        error ->
            {ok, Filter#{field=>Field, type => maps:get(Field, Types, string)}}
    end.

%% @private
filter_op(#{op:=_}=Filter) ->
    Filter;

filter_op(#{value:=Value}=Filter) ->
    {Op, Value2} = expand_op(Value),
    Filter#{op=>Op, value:=Value2}.



%%%% Adds fields_filter and fields_sort
%%analyze(Spec) ->
%%    Filter1 = maps:get(filter, Spec, #{}),
%%    Filter2 =
%%        maps:get('and', Filter1, []) ++
%%        maps:get('or', Filter1, []) ++
%%        maps:get('not', Filter1, []),
%%    FilterFields = [{Field, Op} || #{field:=Field, op:=Op} <- Filter2],
%%    Sort = maps:get(sort, Spec, []),
%%    SortFields = [Field || #{field:=Field} <-Sort],
%%    Spec#{fields_filter=>FilterFields, fields_sort=>SortFields}.


%% ===================================================================
%% Param-based search
%% ===================================================================


parse_params(Spec, #{params:=Params}) ->
    Syntax = #{
        from => pos_integer,
        size => pos_integer,
        sort => #{'__map_binary' => {atom, [asc, desc]}},
        labels => #{'__map_binary' => binary},
        fields => #{'__map_binary' => binary},
        links => #{'__map_binary' => binary},
        fts => #{'__map_binary' => binary},
        deep => boolean,
        get_total => boolean,
        get_data => boolean,
        get_metadata => boolean
    },
    try
        case nklib_syntax:parse_all(Params, Syntax) of
            {ok, ParsedParams} ->
                {ok, do_parse_params(maps:to_list(ParsedParams), Spec)};
            {error, Error} ->
                {error, Error}
        end
    catch
        throw:Throw ->
            Throw
    end;

parse_params(Spec, _Opts) ->
    {ok, Spec}.


%% @private
do_parse_params([], Spec) ->
    Spec;

do_parse_params([{Key, Val}|Rest], Spec)
    when Key==namespace; Key==from; Key==size; Key==deep; Key==get_total; Key==get_data;
         Key==get_metadata ->
    do_parse_params(Rest, Spec#{Key => Val});

do_parse_params([{fields, Map}|Rest], Spec) ->
    Spec2 = parse_filter(maps:to_list(Map), Spec),
    do_parse_params(Rest, Spec2);

do_parse_params([{labels, Map}|Rest], Spec) ->
    Spec2 = parse_labels(maps:to_list(Map), Spec),
    do_parse_params(Rest, Spec2);

do_parse_params([{fts, Map}|Rest], Spec) ->
    Spec2 = parse_fts(maps:to_list(Map), Spec),
    do_parse_params(Rest, Spec2);

do_parse_params([{links, Map}|Rest], Spec) ->
    Spec2 = parse_links(maps:to_list(Map), Spec),
    do_parse_params(Rest, Spec2);

do_parse_params([{sort, Map}|Rest], Spec) ->
    Sort = parse_sort(maps:to_list(Map), []),
    do_parse_params(Rest, Spec#{sort=>Sort});

do_parse_params([_|Rest], Spec) ->
    do_parse_params(Rest, Spec).


%% @private
parse_filter([], Spec) ->
    Spec;

parse_filter([{Field, Value}|Rest], Spec) ->
    Spec2 = parse_field_op(Field, Value, Spec),
    parse_filter(Rest, Spec2).


%% @private
parse_field_op(Field, Value, Spec) ->
    {Op, Value2} = expand_op(Value),
    Filter = #{field=>Field, op=>Op, value=>Value2},
    add_and_filter(Filter, Spec).


%% @private
parse_labels([], Spec) ->
    Spec;

parse_labels([{Field, Value}|Rest], Spec) when Value == <<>> ->
    Field2 = <<"metadata.labels.", Field/binary>>,
    Spec2 = add_and_filter(#{field=>Field2, op=>exists, value=>true}, Spec),
    parse_labels(Rest, Spec2);

parse_labels([{Field, Value}|Rest], Spec) ->
    Field2 = <<"metadata.labels.", Field/binary>>,
    Spec2 = add_and_filter(#{field=>Field2, op=>eq, value=>Value}, Spec),
    parse_labels(Rest, Spec2).


%% @private
parse_links([], Spec) ->
    Spec;

parse_links([{UID, Type}|Rest], Spec) when Type == <<>> ->
    Field2 = <<"metadata.links.", UID/binary>>,
    Spec2 = add_and_filter(#{field=>Field2, op=>exists, value=>true}, Spec),
    parse_links(Rest, Spec2);

parse_links([{UID, Type}|Rest], Spec) ->
    Field2 = <<"metadata.links.", UID/binary>>,
    Spec2 = add_and_filter(#{field=>Field2, op=>eq, value=>Type}, Spec),
    parse_links(Rest, Spec2).


%% @private
parse_fts([], Spec) ->
    Spec;

parse_fts([{Field, Value}|Rest], Spec) ->
    Field2 = <<"metadata.fts.", Field/binary>>,
    Filter =case binary:split(Value, <<"*">>) of
        [V1, <<>>] ->
            #{field=>Field2, op=>prefix, value=>V1};
        _ ->
            #{field=>Field2, op=>eq, value=>Value}
    end,
    Spec2 = add_and_filter(Filter, Spec),
    parse_fts(Rest, Spec2).


%% @private
parse_sort([], Acc) ->
    lists:reverse(Acc);

parse_sort([{Field, Value}|Rest], Acc) ->
    Sort = #{field=>Field, order=>Value},
    parse_sort(Rest, [Sort|Acc]).


%% @private
expand_op(Value) ->
    case binary:split(to_bin(Value), <<":">>) of
        [Op, Value2] ->
            Op2 = case catch binary_to_existing_atom(Op, latin1) of
                eq -> eq;
                ne -> ne;
                gt -> gt;
                gte -> gte;
                lt -> lt;
                lte -> lte;
                prefix -> prefix;
                values -> values;
                exists -> exists;
                _ -> none
            end,
            case Op2 of
                values ->
                    {values, binary:split(Value2, <<"|">>, [global])};
                exists ->
                    case nklib_util:to_boolean(Value2) of
                        error ->
                            {eq, Value};
                        Bool ->
                            {exists, Bool}
                    end;
                none ->
                    {eq, Value};
                _ ->
                    {Op2, Value2}
            end;
        [<<>>] ->
            {exists, true};
        [Value2] ->
            {eq, Value2}
    end.



%% @private
add_and_filter(Filter, Spec) ->
    SpecFilter = maps:get(filter, Spec, #{}),
    And1 = maps:get('and', SpecFilter, []),
    And2 = [Filter|And1],
    SpecFilter2 = SpecFilter#{'and' => And2},
    Spec#{filter => SpecFilter2}.


%% @private
to_bin(Term) when is_binary(Term) -> Term;
to_bin(Term) -> nklib_util:to_binary(Term).

