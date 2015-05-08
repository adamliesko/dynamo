%% Feel free to use, reuse and abuse the code in this file.

%% @private
-module(dynamo_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

start(_Type, Args) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", root_handler, []}
		]}
	]),
	{ok, _} = cowboy:start_http(http, 100, [{port, 9999}], [
		{env, [{dispatch, Dispatch}]}
	]),
	dynamo_sup:start_link(Args).

stop(_State) ->
	ok.
