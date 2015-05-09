-module(dynamo_sup).
-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Args) ->
	supervisor:start_link(dynamo_sup,Args).

init({N,StorageArgs}) ->
	Procs = [
	    {ring, {ring,start_link,[]}, permanent, 1000, worker, [ring]},
              {director, {director,start_link,[N]}, permanent, 1000, worker, [director]},
              {storage_sup, {storage_sup,start_link,[StorageArgs]}, permanent, 1000, supervisor, [storage_sup]}
	],
	{ok, {{one_for_all, 0, 1}, Procs}}.
