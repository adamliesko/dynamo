% Description %
% Director server as a central coordinating module which accepts calls from Cowboy API and forwards them to the
% underlaying modules - ring, storage, vector clock etc.
-module(director).

-behaviour(gen_server).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([start_link/1, get/1, put/3, del/1, post/3,stop/0]).

%% N - degree of replication
%% R - number of  req. successful replies during read operation
%% W - number of  req. successful replies during read operation
-record(director, {n,r,w}).

% initiates gen server with params n,r,w
start_link({N,R,W}) ->
  error_logger:info_msg("~nStarting a gen server with params: N:~p ,R:~p, W:~p on a node: ~p ~n,", [N,R,W,node()]),
  gen_server:start_link({local, director}, ?MODULE, {N,R,W}, []).

stop() ->
    gen_server:call(director, stop).

% api for getting a key
get(Key) ->
  gen_server:call(director, {get, Key}).

% api for getting a key
del(Key) ->
  gen_server:call(director, {del, Key}).

% api for putting a key, with option to specify context
put(Key, Context, Val) ->
  gen_server:call(director, {put, Key, Context, Val}).

% api for posting a key, with option to specify context
post(Key, Context, Val) ->
  gen_server:call(director, {put, Key, Context, Val}).

% initialize new director record and sets state
init({N,R,W}) ->
    {ok, #director{n=N,r=R,w=W}}.

% Gen server calls - important stuff  is inside p_XXX methods
handle_call({put, Key, Context, Val}, _From, State) ->
  {reply, p_put(Key, Context, Val, State), State};
handle_call({get, Key}, _From, State) ->
  {reply, {ok, p_get(Key, State)}, State};
handle_call({del, Key}, _From, State) ->
  {reply, p_del(Key, State), State};

handle_call({post, Key, Context, Val}, _From, State) ->
  {reply, p_post(Key, Context, Val, State), State};
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

%% Puts a Key inside the correct node, has to receive over W replies in order for a successful reply
% PUTS ALWAYS TRIES TO STORE THE KEY
%% - gets nodes for a selected key
%% - gets partitions for a selected key
%% - parse vector clock and incr
%% - Builds up a function of a put key operation
%% - calls this function over selected Nodes
%% - parse replies from nodes
%% - if over W correct replies -> return ok reply and puts key
p_put(Key, Context, Val, #director{w=W,n=_N}) ->
  error_logger:info_msg("Putting up a key, director on node: ~p", [node()]),
  Nodes = ring:get_nodes_for_key(Key),
  error_logger:info_msg("~nThese are the current nodes~p,", [Nodes]),
  Part = ring:part_for_key(Key),
  error_logger:info_msg("~nThis is the partition fror a key~p~n,", [Part]),
  Incr = if Context == [] -> vector_clock:incr(node(), []);
       true ->  vector_clock:incr(node(), [{node(),Context}])
  end,
  Command = fun(Node) ->
    storage:put({list_to_atom(lists:concat([storage_, Part])),Node}, Key, Incr, Val)
  end,
  {GoodNodes, _Bad} = check_nodes(Command, Nodes),
  error_logger:info_msg("~nThese are the good replies:~p~n,", [GoodNodes]),
  %% check consistency init  param W
  if
    length(GoodNodes) >= W -> {ok,{length(GoodNodes)}};
    true -> {failure,{length(GoodNodes)}}
  end.

%% Posts a Key inside the correct node, has to receive over W replies in order for a successful reply
%% POST STORES KEY ONLY IF IT DOES NOT EXIST PREVIOUSLY
%% - gets nodes for a selected key
%% - gets partitions for a selected key
%% - parse vector clock and incr
%% - Builds up a function of a put key operation
%% - calls this function over selected Nodes
%% - parse replies from nodes
%% - if over W correct replies -> return ok reply and puts key
p_post(Key, Context, Val, #director{w=W,n=_N}) ->
  error_logger:info_msg("Putting up a key, director on node: ~p", [node()]),
  Nodes = ring:get_nodes_for_key(Key),
  error_logger:info_msg("~nThese are the current nodes~p,", [Nodes]),
  Part = ring:part_for_key(Key),
  error_logger:info_msg("~nThis is the partition fror a key~p~n,", [Part]),
  Incr = if Context == [] -> vector_clock:incr(node(), []);
       true ->  vector_clock:incr(node(), [{node(),Context}])
  end,
  Command = fun(Node) ->
    storage:put({list_to_atom(lists:concat([storage_, Part])),Node}, Key, Incr, Val)
  end,
  {GoodNodes, _Bad} = check_nodes(Command, Nodes),
  error_logger:info_msg("~nThese are the good replies:~p~n,", [GoodNodes]),
  %% check consistency init  param W
  if
    length(GoodNodes) >= W -> {ok,{length(GoodNodes)}};
    true -> {failure,{length(GoodNodes)}}
  end.

%% Delete a key inside the correct node, has to receive over W replies in order for a successful reply
%% - gets nodes for a selected key
%% - gets partitions for a selected key
%% - parse vector clock and incr
%% - Builds up a function of a put key operation
%% - calls this function over selected Nodes
%% - parse replies from nodes
%% - if over W correct replies -> return ok reply and delete key
p_del(Key, #director{w=W,n=_N}) ->
  error_logger:info_msg("Deleting a key, director on node: ~p", [node()]),
  Nodes = ring:get_nodes_for_key(Key),
  error_logger:info_msg("~nThese are the current nodes~p,", [Nodes]),
  Part = ring:part_for_key(Key),
  error_logger:info_msg("~nThis is the partition fror a key~p~n,", [Part]),
  Command = fun(Node) ->
    storage:delete({list_to_atom(lists:concat([storage_, Part])),Node}, Key)
  end,
  {GoodNodes, _Bad} = check_nodes(Command, Nodes),
  error_logger:info_msg("~nThese are the good replies:~p~n,", [GoodNodes]),
  %% check consistency init  param W
  if
    length(GoodNodes) >= W -> {ok,{length(GoodNodes)}};
    true -> {failure,{length(GoodNodes)}}
  end.

%% Gets a value for key, has to receive over R replies in order for a successful reply
%% - gets nodes for a selected key
%% - gets partitions for a selected key
%% - parse vector clock and incr
%% - Builds up a function of a get key operation
%% - calls this function over selected Nodes
%% - parse replies from nodes
%% - if over R correct replies -> return ok reply and store key
p_get(Key, #director{r=R,n=_N}) ->
  Nodes = ring:get_nodes_for_key(Key),
  error_logger:info_msg("~nThese are the current nodes~p,", [Nodes]),
  Part = ring:part_for_key(Key),
  error_logger:info_msg("~nThis is the partition fror a key~p~n,", [Part]),
  Command = fun(Node) ->
    storage:get({list_to_atom(lists:concat([storage_, Part])), Node}, Key)
  end,
  {GoodNodes, Bad} = check_nodes(Command, Nodes),
  NotFound = check_not_found(Bad,R),
    %% check consistency init  param R
  error_logger:info_msg("~nThese are the good replies:~p~n,", [GoodNodes]),
  if
    length(GoodNodes) >= R -> {ok, read_replies(GoodNodes)};
    NotFound -> {ok, not_found};
    true -> {failure,{length(GoodNodes)}}
  end.

% this is just to check whether we have received over R replies, marked as a Bad reply.
% this is applied only after not getting over R correct replies
% in case of this is false, we return failure as a reply  for ops GET, POST, PUT, DELETE ...
check_not_found(BadReplies, R) ->
  Total = lists:foldl(fun({_, Elem}, Aku) -> 
    case Elem of
      not_found -> Aku+1;
      _ -> Aku
    end
  end, 0, BadReplies),
  if
    Total >= R -> true;
    true -> false
  end.

% Reads array of replies , and if there is not a not_found reply it folds over the replies and resolves it with a vector clock resolution
read_replies([FirstReply|Replies]) ->
 error_logger:info_msg(" Replies from nodes FR: ~p~n, R:~p~n",[FirstReply,Replies]),
  case FirstReply of
    not_found -> not_found;
    _ -> lists:foldr(fun vector_clock:fix/2, FirstReply, Replies)
  end.

%% Get replies from nodes , takes only good replies and maps them to values
check_nodes(Command, Nodes) ->
  Replies = reader:map_nodes(Command,Nodes),
  GoodReplies = [X|| X <- Replies,get_ok_replies(X) ],
 BadReplies = lists:subtract(Replies,GoodReplies),
  GoodValues = lists:map(fun get_value/1, GoodReplies),
  {GoodValues, BadReplies}.

%% this just truncates the reply and gets value of a key from it
get_value({_, {ok, Val}}) ->
  Val;
  get_value(V) ->
    V.

%% filter only goood replies, failure/not found coming through
%% we can rework it as a list comprehension
get_ok_replies({_, {ok, _}}) ->
  true;
get_ok_replies({_, ok}) ->
  true;
get_ok_replies(_Reply) ->
  false.

%%filter UNUSED(Set1, Fnc) ->
%%  [X || X <- Set1, Fnc(X)].
