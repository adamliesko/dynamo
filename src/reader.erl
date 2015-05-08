-module(reader).
-export([map_nodes/3]).

%% returns Array of fncs
map_nodes(Command, Array, Nodes) ->
  SpawnReader =
    case length(Nodes) of
       0 -> fun spawn/1;
       Size ->
       %% todo, rework this random next following node shit
         FollowingNode = fun() -> lists:nth(random:uniform(Size), Nodes) end,
         fun(X) -> spawn(FollowingNode(), X) end
    end,
  Parent = self(),
  %% get pids
  Pids = [SpawnReader(fun() -> Parent ! {self(), (catch Command(Element))} end)
    || Element <- Array],
    %% final map of nodes and pids
  [receive {Pid, Val} -> Val end || Pid <- Pids].
