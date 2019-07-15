-ifndef(NKACTOR_REQUEST_HRL_).
-define(NKACTOR_REQUEST_HRL_, 1).

-include("nkactor.hrl").



-define(REQ_SPAN, nkactor_request).

-define(REQ_DEBUG(Txt, Args),
    case erlang:get(nkactor_debug) of
        true -> ?REQ_LOG(debug, Txt, Args);
        _ -> ok
    end).

-define(REQ_LOG(Type, Txt, Args),
    lager:Type("NkACTOR REQ " ++ Txt, Args)).


-define(REQ_DEBUG(Txt, Args, Req),
    case erlang:get(nkactor_debug) of
        true -> ?REQ_LOG(debug, Txt, Args, Req);
        _ -> ok
    end).


-define(REQ_LOG(Type, Txt, Args, Req),
    lager:Type(
        [
            {verb, maps:get(verb, Req, get)},
            {group, maps:get(group, Req, <<>>)},
            {resuource, maps:get(resource, Req, <<>>)},
            {name, maps:get(name, Req, <<>>)}
        ],
        "NkACTOR REQ (~s ~s/~s/~s) " ++ Txt,
        [
            maps:get(verb, Req, get),
            maps:get(group, Req, <<>>),
            maps:get(resource, Req, <<>>),
            maps:get(name, Req, <<>>) |
            Args
        ]
    )).

-endif.

