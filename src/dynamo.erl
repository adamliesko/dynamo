-module(dynamo).
-export([start/0]).

start() ->
  application:load(dynamo),
  application:start(dynamo).
