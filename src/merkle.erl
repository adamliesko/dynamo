-module(merkle).
-export([init/2, insert/3, delete/2, delete/3, diff/2]).

-record(root, {min, max, node}).
-record(node, {hash, middle, left, right}).
-record(leaf, {hash, key}).

init(Min, Max) ->
#root{min=Min,max=Max,node=#node{hash=empty,middle=(Min+Max) div 2,left=empty,right=empty}}.

insert(Key, Value, #root{max=Max,min=Min,node=Node}) ->
  #root{node=insert(hash(Key), Key, Value, Min, Max, Node)}.

insert(_, Key, Value, _, _, empty) ->
  #leaf{hash=hash(Value),key=Key};

insert(KeyHash, Key, Value, Min, Max, Node) when is_record(Node, node) ->
  H = hash(Key),
  if (H < Node#node.middle) ->
    Left=insert(KeyHash, Key, Value, Min, Node#node.middle, Node#node.left),
    Node#node{left=Left, right=Node#node.right, hash=hash({hash(Left),hash(Node#node.right)})};
  true ->
    Right=insert(KeyHash, Key, Value, Node#node.middle, Max, Node#node.right),
    Node#node{left=Node#node.left,right=Right,hash=hash({hash(Node#node.left),hash(Right)})}
  end;

insert(_, Key, Value, _, _, Leaf) when is_record(Leaf, leaf) and (Key==Leaf#leaf.key) ->
  #leaf{hash=hash(Value),key=Key}; %replace leaf
insert(KeyHash, Key, Value, Min, Max, Leaf) when is_record(Leaf, leaf) ->
  error_logger:info_msg("~Min a max: ~p a ~p", [Min, Max]),
    Middle=(Min+Max) div 2,
    Node = #node{middle=Middle,left=empty,right=empty,hash=empty},
    H = hash(Leaf#leaf.key),
    SemiNode = if H < Middle -> Node#node{left=Leaf};
               true -> Node#node{right=Leaf}
               end,
    insert(KeyHash, Key, Value, Min, Max, SemiNode).

delete(Key, Root = #root{node=Node}) ->
  Root#root{node=delete(hash(Key), Key, Node)}.

delete(_, _, empty) -> empty;
delete(_, Key, Leaf) when is_record(Leaf, leaf) and (Key == Leaf#leaf.key) -> empty;
delete(_, _, Leaf) when is_record(Leaf, leaf) -> Leaf;
delete(KeyHash, Key, Node) when is_record(Node, node) and (KeyHash < Node#node.middle) -> Node#node{left=delete(KeyHash,Key,Node#node.left),right=Node#node.right};
delete(KeyHash, Key, Node) when is_record(Node, node) -> Node#node{left=Node#node.left, right=delete(KeyHash,Key,Node#node.right)}.

diff(#root{node=NodeA}, #root{node=NodeB}) ->
  diff(NodeA, NodeB);

diff(empty, empty) -> [];
diff(empty, Leaf) when is_record(Leaf, leaf) -> [Leaf#leaf.key];
diff(Leaf, empty) when is_record(Leaf, leaf) -> [Leaf#leaf.key];
diff(empty, Node) when is_record(Node, node) -> diff(empty, Node#node.left) ++ diff(empty, Node#node.right);
diff(Node, empty) when is_record(Node, node) -> diff(Node#node.left, empty) ++ diff(Node#node.right, empty);


% Problem Node and Node
diff(NodeA, NodeB)  when is_record(NodeA, node) and is_record(NodeB, node) ->
  if NodeA#node.hash == NodeB#node.hash -> [];
    true -> diff(NodeA#node.left,NodeB#node.left) ++ diff(NodeA#node.right,NodeB#node.right)
  end;

% Problem Leaf and Node
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

% Problem Leaf and Leaf
diff(LeafA, LeafB) when is_record(LeafA, leaf) and is_record(LeafB, leaf) ->
  if (LeafA#leaf.hash == LeafB#leaf.hash) and (LeafA#leaf.key == LeafB#leaf.key) -> [];
     (LeafA#leaf.hash /= LeafB#leaf.hash) and (LeafA#leaf.key == LeafB#leaf.key) -> [LeafA#leaf.key];
     true -> [LeafA#leaf.key,LeafB#leaf.key]
  end.

%% INTERNAL HASH fnc
hash(#root{node=Node}) ->
 hash(Node);
hash(#node{hash=Hash}) ->
 Hash;
hash(#leaf{hash=Hash}) ->
 Hash;
hash(N) -> erlang:phash2(N).
