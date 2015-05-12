-module(ring).

-behaviour(gen_server).
-define(V_NODES, 64).
%% API


-export([start_link/1, get_range/1, part_for_key/1, parts_for_node/2, get_oldies_parts/0, nodes/0 ,stop/0, get_nodes_for_key/1, join/2, launch_gossip/0, set_state/1, get_state/0 ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-record(ring, {n,q,parts, version, nodes,oldies}).

%% PUBLIC API


get_state() ->
  gen_server:call(ring, get_state).

set_state(State) ->
  gen_server:call(ring, {set_state, State}).

start_link({N,Q}) ->
  gen_server:start({local, ring}, ?MODULE, {N,Q}, []).

%% get the range of hash keys which belong to a certain partition
get_range(Part) ->
  gen_server:call(ring, {get_range, Part}).

part_for_key(Key) ->
  gen_server:call(ring, {part_for_key, Key}).

nodes() ->
  gen_server:call(ring, nodes).

parts_for_node(CurrentNode, Role) ->
  gen_server:call(ring, {parts_for_node, CurrentNode, Role}).

get_nodes_for_key(Key) ->
    gen_server:call(ring, {get_nodes_for_key, Key}).

join(OtherNode,ThisNode) ->
	gen_server:call({ring,OtherNode}, {join, ThisNode}).

get_oldies_parts() ->
  gen_server:call(ring, get_oldies_parts).

stop() ->
  gen_server:call(ring, stop).

%% GEN SERVER calls

init({N,Q}) ->
  CurrentNodes = erlang:nodes(), %% built_in_function truly, shadowing our own? duh
  LofNodes = length(CurrentNodes),
   {ok, State} = if
     %% join random existing nodes
    LofNodes > 0 ->
       join(get_rand_node(CurrentNodes), node());
    true -> {ok, init_state_setup({N,Q})}
   end,
   %% periodically gossip news in our little dynamic worlds
   timer:apply_interval(750, ring, launch_gossip, []),
   {ok, State}.

launch_gossip() ->
  State = get_state(),
  CurrentNodes = ring:nodes() -- [node()],
  LofNodes = length(CurrentNodes),
     UpdatedState = if LofNodes > 0 ->
                     %% call random 2 nodes from ring, to share their state
                     {GoodResponses,_} = gen_server:multi_call(get_rand_nodes(2, CurrentNodes), ring, {share_state, State}),
                     lists:foldl(fun(
                       {_,St}, empty) -> St;
                       %% if there are both states , they should join
                     ({_,St}, OtherState) ->
                       join_states(OtherState, St)
                      end, empty, GoodResponses);
                     true -> State
     end,
     set_state(UpdatedState).

%% adding new node
handle_call({join, NewNode}, _From, State) ->
  UpdatedS = p_join(NewNode, State),
    {reply, {ok, UpdatedS}, UpdatedS};

handle_call({parts_for_node, Node, Role}, _From, State) ->
  {reply, p_parts_for_node(Node, State, Role), State};

handle_call({part_for_key, Key}, _Fr, State) ->
  {reply, p_part_for_key(Key, State), State};

handle_call(get_oldies_parts, _F, S = #ring{oldies=Parts}) ->
  if is_list(Parts) -> {reply, Parts, S};
      true -> {reply, [],S} %% there are NONE :D
  end;
%% shortcuts ruleezzz
handle_call(get_state, _F, S) -> {reply, S, S};
handle_call({set_state, UpdS}, _From, _S) -> {reply, ok, UpdS};

handle_call({get_range, Part}, _Fr, State) ->
  L = nth_power_of_two(32-State#ring.q), %% push quorum
  End = Part + L,
  {reply, {Part, End}, State};

handle_call(nodes, _Fr, St = #ring{nodes=Nodes}) ->
  {reply, Nodes, St};

handle_call({get_nodes_for_key, Key}, _From, State) ->
      {reply, p_nodes_for_key(Key, State), State};

handle_call({share_state, OtherState} ,_From, State) ->
    ComparisonResult = vector_clock:diff(State#ring.version, OtherState#ring.version),
    case ComparisonResult of
      eq -> {reply, State, State}; %% whatever
      leq -> {reply, OtherState, OtherState}; %% we change!
      geq -> {reply, State, State}; %% boring
      _->
        JoinedState = join_states(State, OtherState),
        {reply, JoinedState, JoinedState}
    end;

handle_call(stop, _From, State) ->
	{stop, shutdown, ok, State}.

handle_cast(_,S) ->
  {noreply,S}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_Old, State, _) ->
	{ok, State}.

%%PRIVATE RING BASIC SETUP AND FUNC

init_state_setup({N,Q}) ->
  #ring{ q=Q,n=N, parts=new_parts(Q, node()), version=vector_clock:new(node()),nodes=[node()]}.

new_parts(Quorom, Node) ->
  [{Node,Part} || Part <-  lists:seq(1, nth_power_of_two(32), nth_power_of_two(32-Quorom))].


nth_power_of_two(Exp) ->
  1 bsl Exp.

get_rand_node(List) ->
  %% this is truly random
  Index = random:uniform(length(List)),
  [Node] = 	lists:nth(Index,List),
  Node.

get_rand_nodes(2, List) ->
  %% pseudo random, not at uniform even :D, its faster at least
  [H|T] = List,
  [HB|_] = T,
  [H|HB].

  %% n is from n r w params  - degree of replication
join_states(First, Second) ->
    Nodes = lists:merge(First#ring.nodes, Second#ring.nodes),
    Parts = join_parts(First#ring.parts,Second#ring.parts, [], First#ring.n, Nodes),
    #ring{nodes=Nodes, version=vector_clock:join(First#ring.version, Second#ring.version), parts=Parts, n= First#ring.n, q=First#ring.q}.

join_parts([], [], Acc, _, _) ->
    lists:keysort(2, Acc);
%% end recursive madness, sorting by 2nd
join_parts([], S, Acc, _, _) ->
       lists:keysort(2, S ++ Acc);

  join_parts(F, [], Acc, _, _) ->
 lists:keysort(2, F ++ Acc);



      %% n is from n r w params  - degree of replication
%% join partitions , if same, follow further down the line, if not , choose wisely
join_parts([{FNode,No}|FPart], [{SNode,No}|SPart], Acc, N, CurrentNodes) ->
      if
        FNode == SNode -> join_parts(FPart, SPart, [{FNode,No}|Acc], N, CurrentNodes);
        true ->
          %% is partition within the range  or iniside?
          case inside(N, FNode, SNode, CurrentNodes,nil) of
            {true, F} -> join_parts(FPart, SPart, [{F,No}|Acc], N, CurrentNodes);
            %% it does not matter, select any of them
            _ -> join_parts(FPart, SPart, [{FNode,No}|Acc], N, CurrentNodes)
            %% this i am noot sure
          end
      end.


%% n is from n r w params  - degree of replication - we check if one part
% is inside another
inside(_, _, _, [], _) -> false;
inside(N, FNode, SNode, [H|TNodes], nil) ->
        case H of
          FNode -> inside(N-1, SNode, nil, TNodes,FNode);
          SNode -> inside(N-1, FNode, nil, TNodes, SNode);
          _ -> inside(N-1, FNode, SNode, TNodes, nil)
        end;

inside(0, _, _, _, _) -> false;

inside(N, Tail, nil, [H|TNodes], One) ->
      case H of
          Tail -> {true, One};
          _ -> inside(N-1, Tail, nil, TNodes, One)
        end.

%% select partition for a hashed key
p_part_for_key(Key, State) ->
  Hashed = erlang:phash2(Key),
  Quorum = State#ring.q,
  RangeL = nth_power_of_two(32-Quorum), %% todo, extract? maybe?
  DivRes = (Hashed div RangeL),
  Leftie = (Hashed rem RangeL),
  if
    Leftie > 0 -> DivRes * RangeL + 1;
      true -> ((DivRes-1) * RangeL) + 1
  end.

idx_for_part(Part, Quorom) ->
  RangeL = nth_power_of_two(32-Quorom),
  (Part div RangeL) + 1. %% not zero index


%% retun n cons. nodes, if larger than length then return all, Acc blank else
n_cons_nodes(StartN, No, CNodes) ->
  if
      No >= length(CNodes) -> CNodes;
        true -> n_cons_nodes(StartN, No, CNodes, [], CNodes)
  end.

  take_parts(_, 0, _, _, _, Parts, Taken) ->
    lists:keysort(2, Parts ++ Taken);

  take_parts(CNode, Handouts, 0, PerNode, [H|Nodes], Parts, Taken) ->
    if
      length(Nodes) > 0 -> take_parts(CNode, Handouts, PerNode, PerNode, Nodes,Parts,Taken);
      true -> take_parts(CNode, Handouts, PerNode, PerNode, [H|Nodes], Parts,Taken)
    end;

  take_parts(CNode, Handouts, FromCurr, FromEvery, [H|Nodes], Parts, Taken) ->
    case lists:keytake(H, 1, Parts) of
      {value, {H, Part}, UpdParts} ->
        take_parts(CNode, Handouts-1, FromCurr-1, FromEvery, [H|Nodes], UpdParts, [{CNode,Part}|Taken]);
      false -> take_parts(CNode, Handouts, FromCurr, FromEvery, [H|Nodes], Parts, Taken)    % we are not alone, be nice and dont cut corners
    end.

%% tail call so reverse, to check
n_cons_nodes(_, 0, _, Acc, _) -> lists:reverse(Acc);

n_cons_nodes(FNode, No, [], Acc, CNodes) -> n_cons_nodes(FNode, No, CNodes, Acc, CNodes);

n_cons_nodes(found, N, [H|CNodes], Acc, Nodes) ->
  n_cons_nodes(found, N-1, CNodes, [H|Acc], Nodes);

n_cons_nodes(StartN, N, [StartN|Nodes], Acc, CNodes) ->
  n_cons_nodes(found, N-1, Nodes, [StartN|Acc], CNodes);

n_cons_nodes(StartN, No, [_|CNodes], Acc, Nodes) ->
  n_cons_nodes(StartN, No, CNodes, Acc, Nodes). %% sometimes it could help


p_join(IncomingNode, #ring{n=N,q=Q,parts=Parts,version=Version,nodes=Oldies}) ->
    CurrNodes = lists:sort([IncomingNode|Oldies]),
    NodesL = length(CurrNodes),
    ToHandout = nth_power_of_two(Q) div NodesL,
    PerNode = ToHandout div (NodesL-1),
    {FreshNodes,_} = [X || X <- CurrNodes, fun() -> X =/= IncomingNode end], %%todo, check
    UP = take_parts(IncomingNode, ToHandout, PerNode, PerNode, FreshNodes, Parts, []),
    #ring{n=N,q=Q, parts=UP,version = vector_clock:incr(node(), Version),
      nodes=CurrNodes,oldies=Parts}.


p_parts_for_node(Node, St, master) ->
        Parts = St#ring.parts,
        {Suitable,_} = lists:partition(fun({Nod,_}) -> Nod == Node end, Parts),
        lists:map(fun({_,Part}) -> Part end, Suitable);
p_parts_for_node(Node, St, all) ->
        N = St#ring.n,
        RNodes = lists:reverse(St#ring.nodes),
        PNodes = n_cons_nodes(Node, N, RNodes), %% get N nodes from Node , take from RNodes
        lists:foldl(fun(X, Accu) ->
            lists:merge(Accu, p_parts_for_node(X, St, master)) %% only a pseudo master
          end, [], PNodes).

      p_nodes_for_key(Key, St) ->
        HashedKey= erlang:phash2(Key),
        Quorum = St#ring.q,
        Part = select_part(HashedKey, Quorum),
        p_nodes_for_part(Part, St).

      p_nodes_for_part(Part, St) ->
        Parts = St#ring.parts,
        Quorum = St#ring.q,
        N = St#ring.n,
        {CNode,Part} = lists:nth(idx_for_part(Part, Quorum), Parts),
        n_cons_nodes(CNode, N, St#ring.nodes).

select_part(HashedK,Q) ->
  RangeL = nth_power_of_two(32-Q), %% todo, extract? maybe?
  DivRes = (HashedK div RangeL),
  Leftie = (HashedK rem RangeL),
  if
    Leftie > 0 -> DivRes * RangeL + 1;
      true -> ((DivRes-1) * RangeL) + 1
  end.
