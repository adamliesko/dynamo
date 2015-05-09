-module (vector_clock).
-export ([new/1, fix/2, incr/2]).

new(Node) -> [{Node, 1}].

fix({_FClock, _FValues} = First, {_SClock, _SValues} = Second) ->
  ComparisonResult = diff(First,Second),
  case ComparisonResult of
      leq ->
          Second;
      geq ->
        First;
      eq ->
        First;
      _ ->
        join(First,Second)
  end.

%% todo
diff(First, Second) ->
  First,
  Second,
  eq.

%% todo
join(First,Second) ->
  First,
  Second,
  First.

%% increase context value for each
incr(Node, [Context|VectorClocks]) ->
  UpdatedClocks =   [{CurrentNode, CurrentContext + 1} || {CurrentNode,CurrentContext} <- VectorClocks],
  FinalClocks = [UpdatedClocks | {Node,1}],
  [Context | FinalClocks].
