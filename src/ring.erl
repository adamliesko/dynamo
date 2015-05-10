-module(ring).

-behaviour(gen_server).
-define(V_NODES, 64).
-define(NODES_OFFSET, 1).

-export([start_link/0, hash/0,stop/0, select_node_for_key/1,select_node_for_key/2, join/1 ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-record(ring, {parts, clock_version}).

%% PUBLIC API

start_link() ->
  gen_server:start({local, ring}, ?MODULE, [], []).

join(Node) ->
	gen_server:multi_call({ring,Node}, {join, node()}).

hash() ->
	gen_server:call(ring, hash).

stop() ->
  gen_server:call(ring, stop).

select_node_for_key(Key) ->
	gen_server:call(ring, {select_node_for_key, Key, 1}).
select_node_for_key(Key, N) ->
    	gen_server:call(ring, {select_node_for_key, Key,N}).

fire_gossip() ->
  gen_server:call(ring, init_gossip).

%% GEN SERVER calls

%% merging states of nodes into MergedS and setting state= MergedS if any nodes exist
init(Args) ->
    NodesAtStart = nodes(),
    State = set_ring_state([]),
    Length=length(nodes()),
    if
        Length (Nodes)> 0 ->
              {ok, State} = join(select_node());

    	true -> {ok, initialize_state(Args)}
    end.

%% adding new node
handle_call({join, {Node}, _From, State) ->
  UpdatedState = join(Node,State,nodes_in_order()),
    {reply, UpdatedS, UpdatedS};

handle_call({share_state,UpdatedState}, _From, State) ->
      UpdatedState = join(Node,State,nodes_in_order()),
        {reply, UpdatedS, UpdatedS};

handle_call(hash, _From, State) ->
	{reply, State#ring.hash, State};

%%merging states of nodes - INTERNAL CALL withing init
handle_call({merge,ExternalState}, _From, State) ->
    UpdatedS = p_merge(ExternalState,State),
	{reply, State#ring.hash, UpdatedS};

%% used in R/W operations
handle_call({select_node_for_key, Key, N}, _From, State) ->
	HashedKey = erlang:phash2(Key),
	{reply, nearest_node(HashedKey, N, State), State};
handle_call(stop, _From, State) ->
	{stop, shutdown, ok, State}.

%% async joining
handle_cast({join,Node}, State) ->
    {_, UpdatedS} = p_join(Node,State),
    {noreply, UpdatedS}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _) ->
	{ok, State}.

%%PRIVATE RING BASIC SETUP AND FUNC

set_ring_state(Nodes) ->
	set_ring_state(Nodes, [], dict:new()).

set_ring_state([], HashedRing, Nodes) ->
	#ring{hash=HashedRing,nodes=Nodes};

set_ring_state([Node|T], HashRing, Nodes) ->
	VNodes = v_nodes(Node),
	set_ring_state(T,
		add_nodes(VNodes, HashRing),
		map_nodes(VNodes, Node, Nodes)).

%% merge states of nodes
p_merge(#ring{nodes=NodesA,hash=HashA}, #ring{nodes=NodesB,hash=HashB}) ->
  MergedNodes = dict:merge(fun(_Key,Val1,_Val2) ->
      Val1
    end, NodesA, NodesB),
  MergedHash = lists:umerge(HashA,HashB),
  #ring{nodes=MergedNodes,hash= MergedHash}.

%% add new node
p_join({Name,Node}, State) ->
	case node_inside_ring({Name,Node}, State) of
		true -> {duplicate,State};
		false ->
			VNodes = v_nodes({Name,Node}),
			{reply, success, #ring{
				hash=add_nodes(VNodes, State#ring.hash),
				nodes=map_nodes(VNodes, {Name,Node}, State#ring.nodes)
			}}
	end.

node_inside_ring({Name, Node}, #ring{nodes=Nodes}) ->
  [NodeKey|_] = v_nodes({Name,Node}),
  dict:is_key(NodeKey, Nodes).

%% set ring nodes

%% map with Hash,SequenceOffset

v_nodes({Name,Node})  ->
	lists:map(
		fun(X) ->
			erlang:phash2([X|lists:concat([Name, ":at:", Node])])  %% map to list with hash
		end,
		lists:seq(1, ?V_NODES * ?NODES_OFFSET) %% map to list with generated sequence
	).


%% store nodes into nodesMap
map_nodes(VNodes, Node, Nodes) ->
	lists:foldl(
		fun(VNode, Acc) ->
			dict:store(VNode, Node, Acc)
		end, Nodes, VNodes
	).

%% used when initializing ring state , add array of nodes which is not sorted at beginning
add_nodes(VNodes, HashRing) ->
	lists:merge(lists:sort(VNodes), HashRing).

%% consistent_hashing fast lookup  jumping to following ring nodes

nearest_node(Code, N, #ring{hash=Ring, nodes=Nodes}=State) ->
	NodeCode = case nearest_node(Code, Ring) of
		first -> nearest_node(0, Ring);
		FCode -> FCode
	end,
  NodeName = dict:fetch(NodeCode, Nodes),
  	{removed, UpdatedState} = delete_node(NodeName, State),
	[dict:fetch(NodeCode, Nodes) | nearest_node(NodeCode, N-1,UpdatedState)];
nearest_node(_C, 0, _S) ->
    [].


nearest_node(_, []) -> first;

nearest_node(Code, [NodeKey|T]) ->
	case Code < NodeKey of
		true -> NodeKey;
		false -> nearest_node(Code, T)
	end.

delete_node(NodeName, State) ->
    case node_inside_ring(NodeName, State) of
      true ->
        VNodes = v_nodes(NodeName),
        {removed, #ring{
          hash=remove_hash_nodes(VNodes, State#ring.hash),
          nodes=remove_nodes(VNodes, State#ring.nodes)
        }};
        false -> {not_found, State}
    end.

remove_hash_nodes(VNodes, Hash) ->
  lists:subtract(Hash, VNodes).

remove_nodes(VNodes, Nodes) ->
  lists:foldl(
    fun(VNode, Acc) ->
    	dict:erase(VNode, Acc)
    end, Nodes, VNodes
  ).
