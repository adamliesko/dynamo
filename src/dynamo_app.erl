%% Feel free to use, reuse and abuse the code in this file.

%% @private
-module(dynamo_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

start(_Type, _Args) ->
	Dispatch = cowboy_router:compile([
		{'_', [
			{"/", root_handler, []}
		]}
	]),
	{ok, _} = cowboy:start_http(http, 100, [{port, 8080}], [
		{env, [{dispatch, Dispatch}]}
	]),
	 {ok, Pid} = dynamo_sup:start_link(),
	 {ok,Pid}.

stop(_State) ->
	ok.
