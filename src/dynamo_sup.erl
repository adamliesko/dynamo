%% Description %%
%% Main dynamo supervisor - it starts up other modules: ring , director and storage_sup (storage engine).
%% We chose one_for_one restart strategy and longer timeouts.
-module(dynamo_sup).

%% quorum like parameter
-define(Q, 6).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

start_link(Args) ->
	supervisor:start_link(dynamo_sup,Args).

%% Starts up required modules - ring, director and storage supervisor. Uses NRW params with additional Q param - used
%% for ensuring quorum in app ring
init({{N,R,W},StorageArgs}) ->
	Procs = [
	  {ring, {ring,start_link,[{N,6}]}, permanent, 1000, worker, [ring]},
  	{director, {director,start_link,[{N,R,W}]}, permanent, 1000, worker, [director]},
  	{storage_sup, {storage_sup,start_link,[StorageArgs]}, permanent, 10000, supervisor, [storage_sup]}
	],
	{ok, {{one_for_one,10,1}, Procs}}.
