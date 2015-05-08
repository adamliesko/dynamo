%% Feel free to use, reuse and abuse the code in this file.

%% @doc GET echo handler.
-module(root_handler).
-behaviour(cowboy_http_handler).
-export([handle/2]).
-export([terminate/3]).
-export([init/3]).

-record(state, {
}).

init(_, Req, _Opts) ->
	{ok, Req, #state{}}.
handle(Req,State=#state{}) ->
	Method = cowboy_req:method(Req),
	#{echo := Echo} = cowboy_req:match_qs([echo], Req),
	Req2 = echo(Method, Echo, Req),
	{ok, Req2, State}.

echo(<<"GET">>, undefined, Req) ->
	cowboy_req:reply(400, [], <<"Missing echo parameter.">>, Req);
echo(<<"GET">>, Echo, Req) ->
	cowboy_req:reply(200, [
		{<<"content-type">>, <<"text/plain; charset=utf-8">>}
	], Echo, Req);
echo(_, _, Req) ->
	%% Method not allowed.
	cowboy_req:reply(405, Req).

terminate(_Reason, _Req, _State) ->
	ok.
