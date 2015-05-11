-module(merkle).
-export([init/2, insert/3, delete/2, delete/3, diff/2]).

-record(root, {min, max, node}).
-record(node, {hash, middle, left, right}).
-record(leaf, {hash, key}).

-define(OFFSET_BASIS, 2166136261).
-define(FNV_PRIME, 16777619).

init(Min, Max) ->
Root = #root{min=Min,max=Max,node=#node{hash=empty,middle=(Min+Max) div 2,left=empty,right=empty}}.

%Insert item to tree
insert(Key, Value, Root = #root{max=Max,min=Min,node=Node}) ->
  Root#root{node=insert(hash(Key), Key, Value, Min, Max, Node)}.

% Problem when node is empty
insert(_, Key, Value, _, _, empty) ->
  #leaf{hash=hash(Value),key=Key};

% Problem when node is inner node
insert(KeyHash, Key, Value, Min, Max, Node) when is_record(Node, node) -> 
  H = hash(Key),
  if (H < Node#node.middle) ->
    Left=insert(KeyHash, Key, Value, Min, Node#node.middle, Node#node.left),
    Node#node{left=Left, right=Node#node.right, hash=hash({hash(Left),hash(Node#node.right)})};
  true ->
    Right=insert(KeyHash, Key, Value, Node#node.middle, Max, Node#node.right),
    Node#node{left=Node#node.left,right=Right,hash=hash({hash(Node#node.left),hash(Right)})}
  end;

% Problem when node id leaf
insert(_, Key, Value, _, _, Leaf) when is_record(Leaf, leaf) and (Key==Leaf#leaf.key) ->
  #leaf{hash=hash(Value),key=Key}; %replace leaf
insert(KeyHash, Key, Value, Min, Max, Leaf) when is_record(Leaf, leaf) ->
    Middle=(Min+Max) div 2,
    Node = #node{middle=Middle,left=empty,right=empty,hash=empty},
    H = hash(Leaf#leaf.key),
    SemiNode = if H < Middle -> Node#node{left=Leaf};
               true -> Node#node{right=Leaf}
               end,
    insert(KeyHash, Key, Value, Min, Max, SemiNode).

%Delete item from tree
delete(Key, Root = #root{node=Node}) ->
  Root#root{node=delete(hash(Key), Key, Node)}.

delete(_, _, empty) -> empty;
delete(_, Key, Leaf) when is_record(Leaf, leaf) and (Key == Leaf#leaf.key) -> empty;
delete(_, _, Leaf) when is_record(Leaf, leaf) -> Leaf;
delete(KeyHash, Key, Node) when is_record(Node, node) and (KeyHash < Node#node.middle) -> Node#node{left=delete(KeyHash,Key,Node#node.left),right=Node#node.right};
delete(KeyHash, Key, Node) when is_record(Node, node) -> Node#node{left=Node#node.left, right=delete(KeyHash,Key,Node#node.right)}.

%Diff two roots
diff(#root{node=NodeA}, #root{node=NodeB}) ->
  diff(NodeA, NodeB);

%Problem with empty one
diff(empty, empty) -> [];
diff(empty, Leaf) when is_record(Leaf, leaf) -> [Leaf#leaf.key];
diff(Leaf, empty) when is_record(Leaf, leaf) -> [Leaf#leaf.key];
diff(empty, Node) when is_record(Node, node) -> diff(empty, Node#node.left) ++ diff(empty, Node#node.right);
diff(Node, empty) when is_record(Node, node) -> diff(Node#node.left, empty) ++ diff(Node#node.right, empty);

%Problem Node and Node
diff(NodeA, NodeB) when is_record(NodeA, node) and is_record(NodeB, node) ->
    if NodeA#node.hash == NodeB#node.hash  -> [];
    true -> diff(NodeA#node.left,NodeB#node.left) ++ diff(NodeA#node.right, NodeB#node.right)
    end;

%Problem Node and Leaf
diff(Node, Leaf) when is_record(Node, node) and is_record(Leaf, leaf) ->
  H = hash(Leaf#leaf.key),
  if H < Node#node.middle -> diff(Node#node.left, Leaf) ++ diff(empty, Node#node.right);
  true -> diff(Node#node.right, Leaf) ++ diff(empty, Node#node.left)
  end;
diff(Leaf, Node) when is_record(Node, node) and is_record(Leaf, leaf) ->
  H = hash(Leaf#leaf.key),
  if H < Node#node.middle -> diff(Leaf, Node#node.left) ++ diff(empty, Node#node.right);
  true -> diff(Leaf, Node#node.right) ++ diff(empty, Node#node.left)
  end;

%Problem Leaf - Leaf
diff(LeafA, LeafB) when is_record(LeafA, leaf) and is_record(LeafB, leaf)->
  if
    (LeafA#leaf.hash == LeafB#leaf.hash) and (LeafA#leaf.key == LeafB#leaf.key) -> [];
    (LeafA#leaf.hash /= LeafB#leaf.hash) and (LeafA#leaf.key == LeafA#leaf.key) -> [LeafA#leaf.key];
  true -> [LeafA#leaf.key,LeafB#leaf.key]
  end.

%%%% Internal functions %%%%
hash(#root{node=Node}) -> hash(Node);
hash(#node{hash=Hash}) -> Hash;
hash(#leaf{hash=Hash}) -> Hash;
hash(N) -> hasha(N).
  
%32 bit fnv.
hasha(Term) when is_binary(Term) ->
  fnv_int(?OFFSET_BASIS, Term);
  
hasha(Term) ->
  fnv_int(?OFFSET_BASIS, term_to_binary(Term)).
  
fnv_int(Hash, <<"">>) -> Hash;
  
fnv_int(Hash, <<Octet:8, Bin/binary>>) ->
  Xord = Hash bxor Octet,
  fnv_int((Xord * ?FNV_PRIME) rem (2 bsl 31), Bin).

%md5([]) -> [];
%md5(S) ->
% string:to_upper(
%  lists:flatten([io_lib:format("~2.16.0b",[N]) || <<N>> <= erlang:md5(S)])).



