-module (dict_memory_storage).
-export ([open/2, close/1, get/2, put/4,fold/3]).


%% Description %%
%% Dead simple in memory storage module implemented as a erlang:dict(). Started by respectful nodes of dynamo cluster.

% Initiates new dictionary
open(_Name,_Dir) ->
  dict:new().

% Closes existing dictionary
close(_StorageTable) -> ok.

% Stores a key into dictionary
put(Key, Version, Val, StorageTable) ->
	{ok, dict:store(Key, {Version,[Val]}, StorageTable)}.

% Gets a key from dictionary, in case it can not be found returns -> not_found
get(Key, StorageTable) ->
  case dict:find(Key, StorageTable) of
    {ok, Val} -> {ok, Val};
      _ -> not_found
  end.

% Fold whole dictionary with an Incoming function. Used in building merkle trees during storage init setup.
fold(Cmd, Storehouse, Acc) ->
  dict:fold(fun(Key, {Context, [Value]}, Aku) ->
    Cmd({Key, Context, Value}, Aku)
  end, Acc, Storehouse).
