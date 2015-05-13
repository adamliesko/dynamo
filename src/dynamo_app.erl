%% Feel free to use, reuse and abuse the code in this file.

%% @private
-module(dynamo_app).
-behaviour(application).

%% API.
-export([start/2]).
-export([stop/1]).

%% API.

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
		net_adm:ping('dynamo@127.0.0.1'),
	dynamo_sup:start_link(Args).


stop(_State) ->
	ok.
