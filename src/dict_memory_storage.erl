-module (dict_memory_storage).
-export ([open/2, close/1, get/2, put/4,fold/3]).

open(_Name,_Dir) ->
	L = [{"1", {1,["A"]}}],
 dict:from_list(L).
close(_StorageTable) -> ok.

put(Key, Version, Val, StorageTable) ->
	{ok, dict:store(Key, {Version,[Val]}, StorageTable)}.

get(Key, StorageTable) ->
	case dict:find(Key, StorageTable) of
    {ok, Val} -> {ok, Val};
    _ -> {ok, not_found}
  end.

fold(Cmd, Storehouse, Acc) ->
  dict:fold(fun(Key, {Context, [Value]}, Aku) ->
      Cmd({Key, Context, Value}, Aku)
    end, Acc, Storehouse).
