-module (dict_memory_storage).
-export ([open/1, close/1, get/2, put/3]).

open(_Name) -> dict:new().
close(_StorageTable) -> ok.

put(Key, Val, StorageTable) ->
	dict:store(Key, Val, StorageTable).

get(Key, StorageTable) ->
	dict:fetch(Key, StorageTable).
