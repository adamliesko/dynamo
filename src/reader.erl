%% Description %%
%% An universal reader module with single responsibility of calling command on other nodes.

-module(reader).
-export([map_nodes/2]).


% take function Command as an argument and calls it over the Ndoes Array. Returns responses for the function calls
map_nodes(Command, Array) ->
  Parent = self(),
  %% get pids
  Pids = [spawn(fun() -> Parent ! {self(), { Nod, (catch Command(Nod)) }  } end)||Nod <- Array],
  [receive {Pid, Value} -> Value end || Pid <- Pids].
