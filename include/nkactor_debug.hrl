-ifndef(nkactor_DEBUG_HRL_).
-define(nkactor_DEBUG_HRL_, 1).

%% ===================================================================
%% Defines
%% ===================================================================

-include("nkactor.hrl").

-define(ACTOR_DEBUG(Txt, Args),
    case erlang:get(nkactor_debug) of
        true -> ?ACTOR_LOG(debug, Txt, Args);
        _ -> ok
    end).


-define(ACTOR_DEBUG(Txt, Args, State),
    case erlang:get(nkactor_debug) of
        true -> ?ACTOR_LOG(debug, Txt, Args, State);
        _ -> ok
    end).


-define(ACTOR_LOG(Type, Txt, Args),
    lager:Type("NkACTOR Actor " ++ Txt, Args)).


-define(ACTOR_LOG(Type, Txt, Args, State),
    lager:Type(
        [
            {group, (State#actor_st.actor_id)#actor_id.group},
            {resource, (State#actor_st.actor_id)#actor_id.resource},
            {name, (State#actor_st.actor_id)#actor_id.name},
            {namespace, (State#actor_st.actor_id)#actor_id.namespace},
            {uid, (State#actor_st.actor_id)#actor_id.uid}
        ],
        "NkACTOR ~s:~s:~s (~s, ~s) " ++ Txt,
        [
            (State#actor_st.actor_id)#actor_id.group,
            (State#actor_st.actor_id)#actor_id.resource,
            (State#actor_st.actor_id)#actor_id.name,
            (State#actor_st.actor_id)#actor_id.namespace,
            (State#actor_st.actor_id)#actor_id.uid | Args
        ]
    )).

-endif.
