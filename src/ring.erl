-module(ring).

-behaviour(gen_server).
%% API


-export([p_join/2,join_parts/5,inside/5,init_state_setup/1,n_cons_nodes/3,new_parts/2,start_link/1, get_range/1, part_for_key/1, parts_for_node/2, get_oldies_parts/0, nodes/0 ,stop/0, get_nodes_for_key/1, join/2, launch_gossip/1, set_state/1, get_state/0 ]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,code_change/3]).

-record(ring, {n,q,parts, version, nodes,oldies}).

%% PUBLIC API

get_state() ->
  gen_server:call(ring, get_state).

set_state(State) ->
  gen_server:call(ring, {set_state, State}).

start_link({N,Q}) ->
  gen_server:start_link({local, ring}, ?MODULE, {N,Q}, []).

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
   timer:apply_after(random:uniform(1000) + 5000, ring, launch_gossip, [random:seed()]),
   {ok, State}.

launch_gossip({F, S, T}) ->
  random:seed(F,S,T),
  State = get_state(),
  CurrentNodes = lists:filter(fun(N) -> N /= node() end, ring:nodes()),
  error_logger:info_msg("~nLaunching gossip  on Node : ~p, with current nodes expect our node:~p~n,", [CurrentNodes]),
  LofNodes = length(CurrentNodes),
     UpdatedState = if 
      LofNodes > 0 ->
                     Node = get_rand_node(CurrentNodes),
                     OtherState = gen_server:call({ring, Node}, {share_state, State}),
        join_states(OtherState, State);
      true -> State
     end,
     set_state(UpdatedState),
     timer:apply_after(5000, ring, launch_gossip, [random:seed()]).

%% adding new node
handle_call({join, NewNode}, {_, _From}, State) ->
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
  (2 bsl (Exp-1)).

get_rand_node(List) ->
  %% this is truly random
  Index = random:uniform(length(List)),
  Node = 	lists:nth(Index,List),
  Node.

  %% n is from n r w params  - degree of replication
join_states(First, Second) ->
    Nodes = lists:usort(lists:merge(First#ring.nodes, Second#ring.nodes)),
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

%% tail call so reverse, to check
n_cons_nodes(_, 0, _, Acc, _) ->
lists:reverse(Acc);

n_cons_nodes(FNode, No, [], Acc, CNodes) ->
  n_cons_nodes(FNode, No, CNodes, Acc, CNodes);

n_cons_nodes(found, N, [H|CNodes], Acc, Nodes) ->
  n_cons_nodes(found, N-1, CNodes, [H|Acc], Nodes);

n_cons_nodes(StartN, N, [StartN|Nodes], Acc, CNodes) ->
  n_cons_nodes(found, N-1, Nodes, [StartN|Acc], CNodes);

n_cons_nodes(StartN, No, [_|CNodes], Acc, Nodes) ->
  n_cons_nodes(StartN, No, CNodes, Acc, Nodes). %% sometimes it could help


take_parts(ToNode, Parts, Nodes, {_N,Q}) ->
  GivenToks = nth_power_of_two(Q) div length(Nodes),
  FromEvery = GivenToks div (length(Nodes)-1),
  case lists:keysearch(ToNode, 1, Parts) of
    {value, _} -> Parts;
    false -> if
      FromEvery == 0 -> take_parts(ToNode, GivenToks, 1, Parts, Nodes, []);
      true -> take_parts(ToNode, GivenToks, FromEvery, Parts, Nodes, [])
    end
  end.

take_parts(_, _, _, Parts, [], Taken) ->
  lists:keysort(2, Parts ++ Taken);  

take_parts(_ToNode, GivenToks, _FromEvery, Parts, _Nodes, Taken) when length(Taken) == GivenToks ->
  timer:sleep(100),
  lists:keysort(2, Parts ++ Taken);

% skip ToNode
take_parts(ToNode, GivenToks, FromEvery, Parts, [ToNode|Nodes], Taken) ->
  take_parts(ToNode, GivenToks, FromEvery, Parts, Nodes, Taken);

take_parts(ToNode, GivenToks, FromEvery, Parts, [FromNode|Nodes], Taken) ->
  {NewParts,NewTaken} = take_n(FromEvery, FromNode, ToNode, Parts, Taken),
  take_parts(ToNode, GivenToks, FromEvery, NewParts, Nodes, NewTaken).


take_n(0, _CurrNode, _ToNode, Parts, Taken) -> {Parts,Taken};

take_n(N, CurrNode, ToNode, Parts, Taken) ->
  case lists:keytake(CurrNode, 1, Parts) of
    {value, {_,P}, NewParts} -> 
      case lists:keytake(CurrNode, 1, NewParts) of
        {value, _, _} -> take_n(N-1, CurrNode, ToNode, NewParts, [{ToNode,P}|Taken]);
        false -> {Parts,Taken}
      end;
    false -> {Parts,Taken}
   end.


p_join(IncomingNode, #ring{n=N,q=Q,parts=Parts,version=Version,nodes=Oldies}) ->
    CurrNodes = lists:sort([IncomingNode|Oldies]),
    UP = take_parts(IncomingNode, Parts, CurrNodes,{N,Q}),
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
    error_logger:info_msg("Part:~p, st: ~p", [Part,St]),
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
