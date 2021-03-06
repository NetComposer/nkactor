-ifndef(NKACTOR_HRL_).
-define(NKACTOR_HRL_, 1).

%% ===================================================================
%% Defines
%% ===================================================================

-define(PACKAGE_CLASS_NKACTOR, <<"Actor">>).

-define(TRACE(Id), nklib_trace:insert(Id, #{})).
-define(TRACE(Id, Meta), nklib_trace:insert(Id, Meta)).


%% ===================================================================
%% Records
%% ===================================================================

-record(actor_id, {
    group :: nkactor:group() | undefined,
    resource :: nkactor:resource() | undefined,
    %vsn :: nkactor:vsn() | undefined,
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
    activated_time :: nklib_util:m_timestamp(),
    links :: nklib_links:links(),
    stop_reason = false :: false | nkserver:msg(),
    unload_policy :: permanent | {expires, nklib_util:m_timestamp()} | {ttl, integer()},
    ttl_timer :: reference() | undefined,
    status_timer :: reference() | undefined,
    parent_span :: nkserver_ot:parent() | undefined
}).


-endif.