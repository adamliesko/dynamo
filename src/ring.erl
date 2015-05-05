-module(ring).

-behaviour(gen_server).
-define(V_NODES, 64).
-define(NODES_OFFSET, 1).

-export([start_link/0, hash/0, select_node_for_key/1, join/1 ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-record(ring, {hash, nodes}).

%% PUBLIC API

start_link() ->
  gen_node:start({local, ring}, ?MODULE, [], []).

join(Node) ->
	gen_node:call(ring, {join, Node}).

hash() ->
	gen_node:call(ring, hash).

select_node_for_key(Key) ->
	gen_node:call(ring, {select_node_for_key, Key}).

%% GEN SERVER calls

init([]) ->
		{ok, set_ring_state([])}.

handle_call({join, Node}, _From, State) ->
	p_join(Node, State);

handle_call(hash, _From, State) ->
	{reply, State#ring.hash, State};

handle_call({select_node_for_key, Key}, _From, State) ->
	KeyHash = erlang:phash2(Key),
	{reply, nearest_node(KeyHash, State), State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _) ->
	{ok, State}.

%%PRIVATE RING BASIC SETUP AND FUNC

set_ring_state(Nodes) ->
	set_ring_state(Nodes, [], dict:new()).

set_ring_state([], HashRing, Nodes) ->
	#ring{hash=HashRing,nodes=Nodes};

set_ring_state([Node|T], HashRing, Nodes) ->
	VNodes = v_nodes(Node),
	set_ring_state(T,
		add_nodes(VNodes, HashRing),
		map_nodes(VNodes, Node, Nodes)).


%% add new node
p_join(Node, State) ->
	case dict:is_key(Node, State#ring.nodes) of
		true -> {reply, exists, State};
		false ->
			VNodes = v_nodes(Node),
			{reply, success, #ring{
				hash=add_nodes(VNodes, State#ring.hash),
				nodes=map_nodes(VNodes, Node, State#ring.nodes)
			}}
	end.

%% set ring nodes

%% map with Hash,SequenceOffset
v_nodes(Node)  ->
	lists:map(
		fun(X) ->
			erlang:phash2([X|atom_to_list(Node)])  %% map to list with hash
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

nearest_node(Code, #ring{hash=Ring, nodes=Nodes}) ->
	NodeCode = case nearest_node(Code, Ring) of
		first -> nearest_node(0, Ring);
		FCode -> FCode
	end,
	dict:fetch(NodeCode, Nodes);

nearest_node(Code, [NodeKey|T]) ->
	case Code < NodeKey of
		true -> NodeKey;
		false -> nearest_node(Code, T)
	end;

nearest_node(_, []) -> first.