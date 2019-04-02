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
-module(nkactor_trace).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([create/0, on/0, off/0, insert/2, insert_traces/1, dump/0]).
-compile(inline).

-define(ETS, nkactor_trace).


%% ===================================================================
%% Public
%% ===================================================================


create() ->
    ets:new(nkactor_trace, [ordered_set, public, named_table]).


on() ->
    put(nkserver_trace, true).


off() ->
    put(nkserver_trace, false).


insert(Id, Meta) ->
    case get(nkserver_trace) of
        true ->
            Time = nklib_date:epoch(usecs),
            Pos = erlang:unique_integer([positive, monotonic]),
            ets:insert(nkactor_trace, {{Time, Pos}, Id, Meta});
        _ ->
            ok
    end.


insert_traces(Traces) ->
    case get(nkserver_trace) of
        true ->
            ets:insert(nkactor_trace, Traces);
        false ->
            ok
    end.


dump() ->
    {_, Lines} = lists:foldl(
        fun({{Time, _Pos}, Log, Meta}, {LastTime, Acc}) ->
            Diff = Time-LastTime,
            M = Diff div 1000,
            U = Diff rem 1000,
            Text = case map_size(Meta)==0 of
                true ->
                    io_lib:format("~p ~6..0B.~3..0B ~s\n", [Time, M, U, Log]);
                false ->
                    io_lib:format("~p ~6..0B.~3..0B ~s ~p\n", [Time, M, U, Log, Meta])
            end,
            {Time, [Text|Acc]}
        end,
        {0, []},
        ets:tab2list(nkactor_trace)),
    ets:delete_all_objects(nkactor_trace),
    lists:reverse(Lines).


