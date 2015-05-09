-module(reader).
-export([map_nodes/2]).

%% returns Array of fncs
map_nodes(Command, Array) ->
  Parent = self(),
  %% get pids
  Pids = [spawn(fun() -> Parent ! {self(), (catch Command(Element))} end)
    || Element <- Array],
    %% final map of nodes and pids
  [receive {Pid, Val} -> Val end || Pid <- Pids].
