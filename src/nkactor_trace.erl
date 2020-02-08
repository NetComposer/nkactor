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

%% @doc Trace Utilities
-module(nkactor_trace).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').
-include("nkactor.hrl").
-export([trace_run/3, trace_event/2, trace_log/2, trace_log/3]).
-export([event/3, log/3, log/4]).
-export([add_trace_meta/2]).


%% ===================================================================
%% Internal
%% ===================================================================


%% @private
trace_run(Name, Fun, #actor_st{srv=SrvId, trace=TraceData}) ->
    Fun2 = fun(TraceId) ->
        put(?MODULE, {SrvId, TraceId}),
        Fun()
    end,
    Opts = maps:with([data, metadata], TraceData),
    nkserver_trace:trace_run(SrvId, Name, Fun2, Opts).


%% @private
trace_event(Type, Data) when is_map(Data) ->
    {SrvId, TraceId} = get(?MODULE),
    nkserver_trace:event(SrvId, TraceId, Type, Data).


%% @private
trace_log(Level, Txt) when is_atom(Level) ->
    trace_log(Level, Txt, []).


%% @private
trace_log(Level, Txt, Args) when is_atom(Level), is_list(Args) ->
    {SrvId, TraceId} = get(?MODULE),
    nkserver_trace:log(SrvId, TraceId, Level, Txt, Args, #{}).


%% @private
event(Type, Data, #actor_st{srv=SrvId, trace=TraceData}) when is_map(Data) ->
    Data1 = maps:get(data, TraceData, #{}),
    Data2 = maps:merge(Data1, Data),
    Meta = maps:get(metadata, TraceData),
    nkserver_trace:event(SrvId, {nkactor_notrace, Meta}, Type, Data2).


%% @private
log(Level, Txt, ActorSt) when is_atom(Level) ->
    log(Level, Txt,[], ActorSt).


%% @private
log(Level, Txt, Args, #actor_st{srv=SrvId, trace=TraceData}) when is_atom(Level), is_list(Args) ->
    Data = maps:get(data, TraceData, #{}),
    Meta = maps:get(metadata, TraceData),
    nkserver_trace:log(SrvId, {nkactor_notrace, Meta}, Level, Txt, Args, Data).


%% @private
add_trace_meta(Data, #actor_st{srv=SrvId, actor_id=ActorId, trace=Trace}=ActorSt) ->
    #actor_id{uid=UID, group=Group, resource=Res} = ActorId,
    Meta = #{
        app => SrvId,
        group => Group,
        resource => Res,
        target => UID
    },
    ActorSt#actor_st{trace=Trace#{data=>Data, metadata=>Meta}}.


%% ===================================================================
%% Internal
%% ===================================================================
%%

%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).
