-module(root_handler).
-export([init/2]).

init(Req, Opts) ->
	Req2 = case cowboy_req:method(Req) of
		<<"GET">> ->
			process_get_request(Req);
		<<"POST">> ->
			process_post_request(Req);
		(_) ->
			cowboy_req:reply(405, Req)
	end,
	{ok, Req2, Opts}.

process_get_request(Req) ->
	#{key := Key} = cowboy_req:match_qs([key], Req),
	if Key > 1 ->
		cowboy_req:reply(200, [
			{<<"content-type">>, <<"text/plain; charset=utf-8">>}
		], Key, Req);
		true->
			cowboy_req:reply(400, [], <<"Missing key parameter.">>, Req)
	end.

process_post_request(Req) ->
	{ok, Params, _Req2}  = cowboy_req:body_qs(Req),
	Key = proplists:get_value(<<"key">>, Params),
	Value = proplists:get_value(<<"value">>, Params),
	if Key > 1 ->
		cowboy_req:reply(200, [
			{<<"content-type">>, <<"text/plain; charset=utf-8">>}
		], Value, Req);
		true->
			cowboy_req:reply(400, [], <<"Missing required parameters.">>, Req)
	end.
