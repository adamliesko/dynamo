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

	%% toto vytiahnut von
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
			%% nech sa moze joinut na ktorykolvek node, nie len pseudo mastera
			%% net_adm:nodes() ziskas mena nodov, ktore bezia v sieti
			%% tak zu si to vies vyskladat {ok,[{"dynamo",54015},{"dynamo2",54022}]} pridanim 127.0.0.1 za tym ( ako v app src )
			%% pripadne, ak dokazeme, zmenit to cele na snames a nemusime pisat hosta ...
			{ok, X} = application:get_env(target),

				   net_adm:ping(list_to_atom(X)),

	dynamo_sup:start_link(Args).


stop(_State) ->
	ok.
