-ifndef(NKACTOR_HRL_).
-define(NKACTOR_HRL_, 1).

%% ===================================================================
%% Defines
%% ===================================================================

-define(NORMALIZE_OPTS, #{allowed=>[$+, $-, $., $_], space=>$_}).

%% ===================================================================
%% Records
%% ===================================================================

-record(actor_id, {
    group :: nkactor:group() | undefined,
    resource :: nkactor:resource() | undefined,
    name :: nkactor:name() | undefined,
    namespace :: nkactor:namespace() | undefined,
    uid :: nkactor:uid() | undefined,
    pid :: pid() | undefined
}).


-record(actor_st, {
    srv :: nkactor:id(),
    module :: module(),
    config :: nkactor:config(),
    actor_id :: #actor_id{},
    actor :: nkactor:actor(),
    run_state :: term(),
    namespace_pid :: pid() | undefined,
    saved_metadata :: map(),
    is_dirty :: true | false | deleted,
    save_timer :: reference(),
    is_enabled :: boolean(),
    %activated_time :: nklib_util:m_timestamp(),
    links :: nklib_links:links(),
    stop_reason = false :: false | nkserver:status(),
    unload_policy :: permanent | {ttl, Msecs::integer()},
    ttl_timer :: reference() | undefined,
    activate_timer :: reference() | undefined,
    expire_timer :: reference() | undefined,
    op_span_ids :: [nkserver_ot:span_id()]
}).


-record(link_info, {
    get_events :: boolean(),
    gen_events :: boolean(),
    avoid_unload :: boolean(),
    data :: term()
}).


-endif.