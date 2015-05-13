-module(director).
-behaviour(gen_server).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, get/1, put/3,stop/0]).
%% n - degree of replication
%%
%% r - consistency between replicas - min number of nodes for read successful operation
%% w - consistency between replicas - min number of nodes for write successful operation
%% r+w > n quorum like system
-record(director, {n,r,w}).

start_link({N,R,W}) ->
  gen_server:start_link({local, director}, ?MODULE, {N,R,W}, []).
stop() ->
    gen_server:call(director, stop).

get(Key) ->
  gen_server:call(director, {get, Key}).

put(Key, Context, Val) ->
  gen_server:call(director, {put, Key, Context, Val}).

init({N,R,W}) ->
    {ok, #director{n=N,r=R,w=W}}.

handle_call({put, Key, Context, Val}, _From, State) ->
  {reply, p_put(Key, Context, Val, State), State};
handle_call({get, Key}, _From, State) ->
  {reply, {ok, p_get(Key, State)}, State};

handle_call(stop, _From, State) ->
  {stop, shutdown, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Inf, State) ->
    {noreply, State}.
terminate(_R, _State) ->
    ok.
code_change(_Old, State, _New) ->
    {ok, State}.

p_put(Key, Context, Val, #director{w=W,n=_N}) ->
  Nodes = ring:get_nodes_for_key(Key),
  Part = ring:part_for_key(Key),
  Incr=vector_clock:incr(node(), [Context]),
  Command = fun(Node) ->
    storage:put({list_to_atom(lists:concat([storage_, Part])),Node}, Key, Incr, Val)
  end,

  {GoodNodes, _Bad} = check_nodes(Command, Nodes),
  %% check consistency init  param W
  if
    length(GoodNodes) >= W -> {ok,{length(GoodNodes)}};
    true -> {failure,{length(GoodNodes)}}
  end.

p_get(Key, #director{r=R,n=_N}) ->
  Nodes = ring:get_nodes_for_key(Key),
  Part = ring:part_for_key(Key),
  Command = fun(Node) ->
    storage:get({list_to_atom(lists:concat([storage_, Part])), Node}, Key)
  end,
  {GoodNodes, Bad} = check_nodes(Command, Nodes),
  NotFound = length(Bad) > 0,
    %% check consistency init  param R
  if
    length(GoodNodes) >= R -> {ok, read_replies(GoodNodes)};
    NotFound -> {ok, not_found};
    true -> {failure,{length(GoodNodes)}}
  end.

read_replies([FirstReply|Replies]) ->
  case FirstReply of
    not_found -> not_found;
    _ -> lists:foldr( fun vector_clock:fix/2, FirstReply, Replies)
  end.

check_nodes(Command, Nodes) ->
  Replies = reader:map_nodes(Command,Nodes),
  GoodReplies = [X|| X <- Replies,get_ok_replies(X) ],
 BadReplies = lists:subtract(Replies,GoodReplies),
  GoodValues = [get_value(X) || X <- GoodReplies],
  {GoodValues, BadReplies}.

get_value({_, {ok, Val}}) ->
  Val;
  get_value(V) ->
    V.
get_ok_replies({_, {ok, _}}) ->
  true;
get_ok_replies({_, ok}) ->
  true;
get_ok_replies(_Reply) ->
  false.


%%filter(Set1, Fnc) ->
%%  [X || X <- Set1, Fnc(X)].
