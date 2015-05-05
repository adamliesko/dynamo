-module(root_handler).
-behaviour(cowboy_http_handler).

-export([init/3]).
-export([handle/2]).
-export([terminate/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{ok, Req, #state{}}.

handle(Req, State=#state{}) ->
	{Method, Req2} = cowboy_req:method(Req),
	
	Params = cowboy_req:match_qs([val,key], Req),

	#{key := Key} = Params,
	#{val := Val} = Params,

    case Method of
        <<"POST">> ->
        	Pid = pg2:get_closest_pid(ring),
    		Res = gen_server:call(Pid, {set, Key, Val}, 1),
          
            {ok, Req3} = cowboy_req:reply(200, [], Res, Req2),
            {ok, Req3, State};
        <<"GET">> ->
        	Pid = pg2:get_closest_pid(ring),
    		Res = gen_server:call(Pid, {get, Key}, 1),

            {ok, Req3} = cowboy_req:reply(200, [], Res, Req2),
            {ok, Req3, State};
         _ ->
         	{ok, Req3} = cowboy_req:reply(200, [], 'else', Req2),
         	{ok, Req3, State}
    end.

terminate(_Reason, _Req, _State) ->
	ok.
