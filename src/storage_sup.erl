%% Description %%
%% Implements a storage engine and watches over actual storage module.
-module(storage_sup).
-behaviour(supervisor).

-export([init/1]).

-export([start_link/1]).

-define(SERVER, ?MODULE).

start_link(_Args) ->
	supervisor:start_link(storage_sup, _Args).

% intializes the storage supervisor - It gets partitions for current node from ring module, gets existing partitions
% from the ring. Then we build up the hash key range using ring:get_range for each partition and initialize our
% storage module - with options respecting the existing partitions.

init(_Args) ->
	Parts = ring:parts_for_node(node(), all),
	OldParts = ring:get_oldies_parts(),

	%% setup every partition
	Setup = lists:map(fun(P) ->

    Title = list_to_atom(lists:concat([storage_, P])),
		{Start,End} = ring:get_range(P),
		case lists:keysearch(P,2,OldParts) of
		%% there are some old partitions
			{Old,_R} -> {P, {storage,start_link, [dict_memory_storage,Title,Title,Start,End,Old]}, permanent, 1000, worker,[storage]};
		%% there are none old partitions which might concern us
			false ->  {P, {storage,start_link, [dict_memory_storage,Title,Title,Start,End]}, permanent, 1000, worker,[storage]}
		end end, Parts),

  {ok,{{one_for_one,0,1},Setup}}.
