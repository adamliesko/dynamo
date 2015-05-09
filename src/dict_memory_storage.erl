-module (dict_memory_storage).
-export ([open/1, close/1, get/2, put/4]).

open(_Name) -> dict:new().
close(_StorageTable) -> ok.

put(Key, Version, Val, StorageTable) ->
	{ok, dict:store(Key, {Version,[Val]}, StorageTable)}.

get(Key, StorageTable) ->
	dict:fetch(Key, StorageTable).
