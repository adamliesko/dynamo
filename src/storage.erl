-module(storage).
-behaviour(gen_server).

-export([start_link/3, get/2, put/3, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

-record(storage, {module,table_storage,name}).

start_link(Storehouse, IdKey, Name) ->
   gen_server:start_link({local, Name}, ?MODULE, {Storehouse,IdKey,Name}, []).

get(Name, Key) ->
	gen_server:call(Name, {get, Key}).

put(Name, Key, Val) ->
	gen_server:call(Name, {put, Key, Val}).

close(Name) ->
    gen_server:call(Name, close).

init({Storehouse,IdKey,Name}) ->
    {ok, #storage{module=Storehouse,table_storage=Storehouse:open(IdKey),name=Name}}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #storage{module=Module,table_storage=TableStorage}) ->
    Module:close(TableStorage).

code_change(_Old, State, _) ->
    {ok, State}.

handle_call({get, Key}, _From, State = #storage{module=Module,table_storage=TableStorage}) ->
	{reply, Module:get(Key, TableStorage), State};

handle_call({put, Key, Val}, _From, State = #storage{module=Module,table_storage=TableStorage}) ->
	{reply, ok, State#storage{table_storage=Module:put(Key,Val,TableStorage)}};

handle_call(close, _From, State) ->
	{stop, close, ok, State}.


