-module(storage_sup).
-behaviour(supervisor).

-export([init/1]).

-export([start_link/1]).

-define(SERVER, ?MODULE).

start_link(_Args) ->
    supervisor:start_link(storage_sup, _Args).

init(_Args) ->
	Parts = ring:parts_for_node(node(), all),
	DeadParts = ring:dead_parts(),
	Setup = lists:map(fun(P) ->
	{Start,End} = ring:range(P),
	
		case lists:keysearch(P,2,DeadParts) of 
			{Dead,_R} -> {P, {storage,start_link, [dict_memory_storage,P,P,Start,End,Dead]}, permanent, 1000, worker,[storage]};
	
			false ->  {P, {storage,start_link, [dict_memory_storage,P,P,Start,End]}, permanent, 1000, worker,[storage]}

		end end, Parts),
     {ok,{{one_for_all,0,1},Setup}}.