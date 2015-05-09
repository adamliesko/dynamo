-module(director).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, get/1, put/3]).

-record(director, {x}).

start_link(X) ->
  gen_server:start_link({local, director}, ?MODULE, X, []).

get(Key) ->
  gen_server:call(director, {get, Key}).

put(Key, Context, Val) ->
  gen_server:call(director, {put, Key, Context, Val}).

init(X) ->
    {ok, #director{x=X}}.

handle_call({put, Key, Context, Val}, _From, State) ->
  {reply, p_put(Key, Context, Val, State), State};
handle_call({get, Key}, _From, State) ->
  {reply, {ok, p_get(Key, State)}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Inf, State) ->
    {noreply, State}.
terminate(_R, _State) ->
    ok.
code_change(_Old, State, _New) ->
    {ok, State}.

p_put(Key, Context, Val, #director{x=X}) ->
  Nodes = ring:select_node_for_key(Key, X),
  Incr=vector_clock:incr(node(), Context),
  Command = fun(Node) ->
    storage:put(Node, Key, Incr, Val)
  end,
  reader:map_nodes(Command, Nodes, []),
  ok.

read([FirstReply|Replies]) ->
  lists:foldr({vector_clock, fix}, FirstReply, Replies).

p_get(Key, #director{x=X}) ->
  Nodes = ring:select_node_for_key(Key, X),
  Command = fun(Node) ->
    storage:get(Node, Key)
  end,
  Replies = reader:map_nodes(Command, Nodes, []),
  OkReplies = lists:filter(fun(Reply) -> {ok,_} = Reply end, Replies),
  Values = lists:map(fun({ok, Value}) -> Value end, OkReplies),
  read(Values).
