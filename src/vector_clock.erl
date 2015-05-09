-module (vector_clock).
-export ([new/1, fix/2, incr/2]).

new(Node) -> [{Node, 1}].

fix({FClock, _FValues} = First, {SClock, _SValues} = Second) ->
  ComparisonResult = diff(FClock,SClock),
  case ComparisonResult of
      leq ->
        Second;
      geq ->
        First;
      eq ->
        First;
      _ ->
        join(FClock,SClock, [])
  end.

%% JOIN -> to be checked

%% keytake(Key, N, TupleList1) -> {value, Tuple, TupleList2} | false
%%  Searches the list of tuples TupleList1 for a tuple whose Nth element
%%  compares equal to Key. %% Returns {value, Tuple, TupleList2} if such a tuple
%%  is found, otherwise false. TupleList2 is a copy of TupleList1 where the
%%  first occurrence of Tuple has been removed.

%% keymerge(N, TupleList1, TupleList2) -> TupleList3
%%  Returns the sorted list formed by merging TupleList1 and TupleList2. The
%%  merge is performed on the Nth element of each tuple. Both TupleList1 and
%%  TupleList2 must be key-sorted prior to evaluating this function. When two
%%  tuples compare equal, the tuple from TupleList1 is picked before the tuple
%%  from TupleList2.

%% keysort(N, TupleList1) -> TupleList2
%%  Returns a list containing the sorted elements of the list TupleList1
%%  Sorting is performed on the Nth element of the tuples. The sort is stable.

join([{FirstNode, VersionFirst}|First], Second,Acc) ->
%% simple pattern matching, check what keytake returns - it remove the element
  case lists:keytake(FirstNode, 1, Second) of
    {value, {FirstNode, VersionSecond}, SecondRest} when VersionFirst > VersionSecond ->
      join(First,SecondRest,[{First,VersionFirst}|Acc]);
    {value, {FirstNode, VersionSecond}, SecondRest}  when VersionSecond >= VersionFirst ->
      join(First,SecondRest,[{First,VersionSecond}|Acc]);
    _ ->
      join(First,Second,[{First,VersionFirst}|Acc])
  end;

join([], Second, Acc) ->
  SortedSecond = lists:keysort(1, Second),
  lists:keymerge(1, Acc, SortedSecond);

join(First, [], Acc) ->
  SortedFirst = lists:keysort(1, First),
  lists:keymerge(1, Acc, SortedFirst).

%% increase context value for each - to be checked
incr(Node, [Context|VectorClocks]) ->
  UpdatedClocks =   [{CurrentNode, CurrentContext + 1} || {CurrentNode,CurrentContext} <- VectorClocks],
  FinalClocks = [UpdatedClocks | {Node,1}],
  [Context | FinalClocks].

equal(First,Second) ->
    if length(First) == length(Second) ->
      lists:all(fun(FirstClock) -> lists:member(FirstClock,Second) end, First);
      true ->
        false
    end.

diff(First,Second) ->
  Eq = equal(First,Second),
  Leq = leq(First,Second),
  Geq = leq(Second,First),
  if
    Eq -> eq;
    Leq -> leq;
    Geq -> geq;
    true -> to_join
  end.


leq(First, Second) ->
  %% first vector clock is shorter
  Shorter = length(First) < length(Second),
  %% it contains the same element but the version is lower
  LessOne = less_one(First,Second),
  %% if all are less or equal
  LessOrEqAll = less_or_eq_all(First,Second),
  (Shorter or LessOne) and LessOrEqAll.


less_one(First, Second) ->
  lists:all(fun({FirstNode,FirstVersion}) ->
  Returned = lists:keysearch(FirstNode, 1, Second),
  case Returned of
    {value,{_SecondNode,SecondVersion}} ->
      SecondVersion >= FirstVersion;
    _ -> false
  end end, First).

less_or_eq_all(First,Second) ->
  lists:any(fun({FirstNode,FirstVersion}) ->
  Returned = lists:keysearch(FirstNode, 1, Second),
  case Returned of
    {value,{_SecondNode,SecondVersion}} ->
      not (SecondVersion == FirstVersion);
    _-> false
  end end, First).
