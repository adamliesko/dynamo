-module(director).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, get/1, put/2]).

-record(director, {x}).

start_link(X) ->
  gen_server:start_link({local, director}, ?MODULE, X, []).

get(Key) ->
  gen_server:call(director, {get, Key}).

put(Key, Val) ->
  gen_server:call(director, {put, Key, Val}).

init(X) ->
    {ok, #director{x=X}}.
    
handle_call({put, Key, Val}, _From, State) ->
  {reply, p_put(Key, Val, State), State};
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

p_put(Key, Val, #director{x=X}) ->
  Nodes = ring:select_node_for_key(Key, X),
  Command = fun(Node) ->
    storage:put(Node, Key, Val)
  end,
  reader:map_nodes(Command, Nodes, []),
  ok.

p_get(Key, #director{x=X}) ->
  Nodes = ring:select_node_for_key(Key, X),
  Command = fun(Node) ->
    storage:get(Node, Key)
  end,
  Responses = reader:map_nodes(Command, Nodes, []),
  [{ok,Val}|_R] = lists:filter(fun(Resp) -> {ok,_} = Resp end, Responses),
  Val.
