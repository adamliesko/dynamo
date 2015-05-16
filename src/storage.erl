-module(storage).
-behaviour(gen_server).

-export([start_link/5,start_link/6, get/2, put/4, close/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, code_change/3, terminate/2]).

%% storage engine definition - important is the module - in our case it is always dict_memory_storage, but we could
%% implement some lightweight database or file system storage. Tree is merkle tree representation of storage data.
-record(storage, {module,table_storage,title,tree}).

% Module is started by storage_sup-it chooses which start_link fnc to call, based upon the existence of some prev. nodes

%% there are no other previous nodes in this certain partition
start_link(Storehouse, IdKey, Title, Start, End) ->
   gen_server:start_link({local, Title}, ?MODULE, {Storehouse,IdKey,Title,Start,End}, []).

%% there is/are some old dead bad wrong thirsty nodes in our partition
start_link(Storehouse, IdKey, Title, Start, End, OldNode) ->
   {ok, This} = gen_server:start_link({local, Title}, ?MODULE, {Storehouse,IdKey,Title,Start,End}, []),
   spawn(fun() -> synchronize(This,{Title,OldNode}) end), %% this is the superb recovery option after failure
   {ok, This}.

%% get storage api endpoint with default timeout value
get(Title, Key) ->
	gen_server:call(Title, {get, Key},1000).

%% put storage api endpoint with default timeout value
put(Title, Key, Version, Val) ->
	gen_server:call(Title, {put, Key,Version, Val},1000).

close(Title) ->
    gen_server:call(Title, close).

%% inits the storage engine, we trap exits inside here. WE open ap a new storage_module defined at startup and create
%% new merkle tree
init({Storehouse,IdKey,Title,Start,End}) ->
    process_flag(trap_exit, true),
    Storage = Storehouse:open(IdKey,Title),
    Tr = Storehouse:fold(fun({Key,_,Val}, Aku) ->
      merkle:insert(Key,Val,Aku) end, Storage, merkle:init(Start,End)),
    {ok, #storage{module=Storehouse,table_storage=Storage,tree=Tr,title=Title}}.

%% boring gen_server calls
handle_cast(_Msg, State) ->
    {noreply, State}.
handle_info(_Info, State) ->
    {noreply, State}.
terminate(_Reason, #storage{module=Module,table_storage=TableStorage}) ->
    Module:close(TableStorage).
code_change(_Old, State, _) ->
    {ok, State}.

%% calls get on a currently used storage module by this certain node
handle_call({get, Key}, _From, State = #storage{module=Module,table_storage=TableStorage}) ->
	{reply,catch Module:get(convert_key_to_list(Key), TableStorage), State};

%% calls put on a currently used storage module by this certain node and updates merkle tree
handle_call({put, Key, Version, Val}, _From, State = #storage{module=Module,table_storage=TableStorage,tree=Tr}) ->
  CurrentTr = merkle:insert(Key,Val,Tr), %% updating tree
  case catch Module:put(convert_key_to_list(Key),Version,Val,TableStorage) of
    {ok, Updated} -> {reply,ok,State#storage{table_storage=Updated,tree=CurrentTr}};
    Failure -> {reply, Failure, State}
  end;

handle_call(close, _From, State) ->
	{stop, shutdown, ok, State};
handle_call(tree,_Fr, State = #storage{tree=T}) ->
  {reply, T,State};

%% used in a merkle trees with director, just fold over the storage module - dict_memory_storage
handle_call({storage_fold,Cmd, Acc}, _Fr, State = #storage{module=Storehouse,table_storage=TableStorage}) ->
  Response = Storehouse:fold(Cmd, TableStorage, Acc),
  {reply, Response, State}.

%% synchronizes storage content of two nodes - used after node failure, when we want to recover data
%% it builds up a new merkle tree
synchronize(First,Second) ->
  FTree  = tree(First),
  STree = tree(Second),
  lists:foreach(fun(Key) ->
    FirstReply = get(First,Key),
    {ok, SecondReply} = get(Second, Key),
    case {FirstReply,SecondReply} of
      %% second is missing some key
        {not_found, {ok, {Version, [Val]}}} ->
        put(First,Key,Version,Val);
        {{ok, {Version, [Val]}}, not_found} ->
      %% first is missing some key
          put(Second,Key,Version,Val);
      %% none, we need to resolve it from vector_clock
      _ ->
        {Version,[Val|_]=Vals} = vector_clock:resolve(FirstReply, SecondReply),
        LengthV = length(Vals),
        if LengthV == 1 ->
          put(Second,Key,Version,Val),
          put(First, Key, Version, Val)
        end
    end
  end, merkle_tree:diff(FTree,STree)).

%% gets current node's merkle tree
tree(Node) ->
  gen_server:call(Node, tree).

%% just a helper fnc to convert incoming key into list as default
convert_key_to_list(Key) when is_atom(Key) ->
   atom_to_list(Key);
convert_key_to_list(Key) when is_binary(Key) ->
  binary_to_list(Key);
convert_key_to_list(Key)  ->
    Key.