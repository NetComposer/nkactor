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

%% @doc NkService registration facilties with cache
%%

-module(nkactor_register).
-author('Carlos Gonzalez <carlosj.gf@gmail.com>').

-export([find_registered/1, find_cached/1]).


-include("nkactor.hrl").
-include_lib("nkserver/include/nkserver.hrl").
-include("nkactor.hrl").
-include_lib("nkactor/include/nkactor_debug.hrl").



%% ===================================================================
%% Public
%% ===================================================================


%% @doc Checks if an actor is activated
%% - checks if it is in the local cache
%% - if not, asks the registration callback

-spec find_registered(nkactor:id()) ->
    {true, #actor_id{}} | false.

find_registered(Id) ->
    ActorId = nkactor_lib:id_to_actor_id(Id),
    case find_cached(ActorId) of
        {true, SrvId, ActorId2} ->
            {true, SrvId, ActorId2};
        false ->
            #actor_id{namespace = Namespace} = ActorId,
            case find_namespace(Namespace) of
                {ok, SrvId, Pid} ->
                    case ?CALL_SRV(SrvId, actor_find_registered, [SrvId, ActorId]) of
                        {true, ActorId2} ->
                            #actor_id{
                                group = Group,
                                resource = Res,
                                name = Name,
                                namespace = Namespace,
                                uid = UID,
                                pid = Pid
                            } = ActorId2,
                            true = is_binary(UID) andalso UID /= <<>>,
                            true = is_pid(Pid),
                            nklib_proc:put({nkactor, Group, Res, Name, Namespace}, {SrvId, ActorId2}, Pid),
                            nklib_proc:put({nkactor_uid, UID}, {SrvId, ActorId2}, Pid),
                            {true, SrvId, ActorId2};
                        false ->
                            {false, SrvId};
                        {error, Error} ->
                            ?ACTOR_LOG(warning, "error calling nkactor_find_actor for ~s: ~p", [Error]),
                            {false, SrvId}
                    end
            end
    end.


%% @private
find_cached(#actor_id{name = <<>>, uid=UID}=ActorId) when UID /= <<>> ->
    case nklib_proc:values({nkactor_uid, UID}) of
        [{{SrvId, ActorId}, _Pid}|_] ->
            {true, SrvId, ActorId};
        [] ->
            false
    end;

find_cached(#actor_id{name=Name}=ActorId) when Name /= <<>> ->
    #actor_id{group=Group, resource=Res, namespace=Namespace} = ActorId,
    case nklib_proc:values({nkactor, Group, Res, Name, Namespace}) of
        [{{SrvId, ActorId2}, _Pid}|_] ->
            {true, SrvId, ActorId2};
        [] ->
            false
    end.


find_namespace(Namespace) ->
    case nklib_proc:values({nkactor_namespace, Namespace}) of
        [{#{srv:=SrvId}, Pid}|_] ->
            {ok, SrvId, Pid};
        [] ->
            case global:whereis_name({nkactor_namespace, Namespace}) of
                Pid when is_pid(Pid) ->
                    {ok, Meta} = gen_server:call(Pid, get_meta),
                    nklib_proc:put({nkactor_namespace, Namespace}, Meta, Pid),
                    {ok, Meta, Pid};
                undefined ->
                    start_namespace(Namespace)
            end
    end.


start_namespace(Namespace) ->
    [Name|Rest] = binary:split(Namespace, <<".">>),
    case find_namespace(Rest) of
        {ok, SrvId, Pid} ->
            do_start_namespace(SrvId, Pid, Name);
        {error, Error} ->
            {error, Error}
    end.


do_start_namespace(SrvId, Pid, _Name) ->
    {ok, SrvId, Pid}.





%%%% @private
%%to_bin(Term) when is_binary(Term) -> Term;
%%to_bin(Term) -> nklib_util:to_binary(Term).