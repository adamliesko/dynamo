-module(storage).
-behaviour(gen_server).

-export([start_link/3, get/2, put/4, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(storage, {module,table_storage,title}).

start_link(Storehouse, IdKey, Title) ->
   gen_server:start_link({local, Title}, ?MODULE, {Storehouse,IdKey,Title}, []).

get(Title, Key) ->
	gen_server:call(Title, {get, Key}).

put(Title, Key, Version, Val) ->
	gen_server:call(Title, {put, Key, Version, Val}).

close(Title) ->
    gen_server:call(Title, close).

init({Storehouse,IdKey,Title}) ->
    process_flag(trap_exit, true),
    ring:join({Title,node()}),
    {ok, #storage{module=Storehouse,table_storage=Storehouse:open(IdKey),title=Title}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #storage{module=Module,table_storage=TableStorage}) ->
    Module:close(TableStorage).

code_change(_Old, State, _) ->
    {ok, State}.

handle_call({get, Key}, _From, State = #storage{module=Module,table_storage=TableStorage}) ->
	{reply,catch Module:get(convert_key_to_list(Key), TableStorage), State};

handle_call({put, Key, Version, Val}, _From, State = #storage{module=Module,table_storage=TableStorage}) ->
  case catch Module:put(convert_key_to_list(Key),Version,Val,TableStorage) of
    {ok, Updated} -> {reply,ok,State#storage{table_storage=Updated}};
    Failure -> {reply, Failure, State}
  end;

handle_call(close, _From, State) ->
	{stop, shutdown, ok, State}.

convert_key_to_list(Key) when is_atom(Key) ->
   atom_to_list(Key);
convert_key_to_list(Key) when is_binary(Key) ->
  binary_to_list(Key);
convert_key_to_list(Key)  ->
    Key.
