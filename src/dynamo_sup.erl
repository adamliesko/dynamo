-module(dynamo_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-spec start_link() -> {ok, pid()}.
start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init({X,StorageArgs}) ->
	Procs = [
	    {ring, {ring,start_link,[]}, permanent, 1, worker, [ring]},
              {director, {director,start_link,[X]}, permanent, 1, worker, [director]},
              {storage_sup, {storage_sup,start_link,[StorageArgs]}, permanent, infinity, supervisor, [storage_sup]}
	],
	{ok, {{one_for_all, 10, 10}, Procs}}.
