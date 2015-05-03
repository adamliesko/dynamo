-module(dynamo_app).
-behaviour(application).

-export([start/2]).
-export([stop/1]).

start(_Type, _Args) ->
    Dispatch = cowboy_router:compile([
        {'_', [{"/get", get_handler, []},{"/put", put_handler, []},{"/post", post_handler, []},{"/", stats_handler, []},
        {"/nodes", nodes_handler, []}]}
    ]),
    {ok, _} = cowboy:start_http(my_http_listener, 100, [{port, 8080}],
        [{env, [{dispatch, Dispatch}]}]
    ),
	dynamo_sup:start_link().
stop(_State) ->
	ok.



