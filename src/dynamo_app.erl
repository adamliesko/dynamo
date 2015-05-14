%% Description %%
%% Main application startup module. It includes pinging of the jointo node and starting up a dynamo supervisor.
-module(dynamo_app).

-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% starts up the cowboy server with carefully selected default - if on a Master node.
%% either way - starts up the dynamo node, and tries to ping the Master node/jointo node
start(_Type, Args) ->
	Node = node(),
	Master = list_to_atom("dynamo@127.0.0.1"),

	if Node == Master ->
			Dispatch = cowboy_router:compile([
				{'_', [
					{"/", root_handler, []}
				]}
			]),
			{ok, _} = cowboy:start_http(http, 100, [{port, 9999}], [
				{env, [{dispatch, Dispatch}]}
			]);
			true ->
				wat
	end,

	{ok, X} = application:get_env(target),
	net_adm:ping(list_to_atom(X)),
	dynamo_sup:start_link(Args).

stop(_State) ->
	ok.
