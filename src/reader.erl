-module(reader).
-export([map_nodes/2]).

map_nodes(Command, Array) ->
  Parent = self(),
  %% get pids
  Pids = [spawn(fun() -> Parent ! {self(), { Nod, (catch Command(Nod)) }  } end)||Nod <- Array],
  [receive {Pid, Value} -> Value end || Pid <- Pids].
