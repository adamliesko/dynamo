-module(storage_sup).
-behaviour(supervisor).

-export([init/1]).

-export([start_link/1]).

-define(SERVER, ?MODULE).

start_link(Args) ->
    supervisor:start_link(storage_sup, Args).

init(Args) ->
    {ok,{{one_for_all,0,1},
      lists:map(fun({Module, IDkey, Title}) ->
          {Title, {storage,start_link,[Module, IDkey, Title]}, permanent, 1000, worker, [storage]}
        end, Args)}}.
