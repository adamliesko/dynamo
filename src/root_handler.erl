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
	case director:get(Key) of
	    {ok, {Context, Values}} ->

				case Context of
					failure -> cowboy_req:reply(400, [], <<"Key not found.">>, Req);
					_ ->
						{_,Value}=Values,
		    	cowboy_req:reply(200, [
					{<<"content-type">>, <<"text/plain; charset=utf-8">>}
				], Value , Req)
			end;
	    {failure, _Reason} ->
	    	cowboy_req:reply(400, [], <<"Missing key parameter.">>, Req)
 	 end.

process_post_request(Req) ->
	{ok, Params, _Req2}  = cowboy_req:body_qs(Req),
	Key = proplists:get_value(<<"key">>, Params),
	Value = proplists:get_value(<<"value">>, Params),
	Context = [],
	case director:put(Key, Context, Value) of
		{ok, _N} ->
			cowboy_req:reply(200, [
				{<<"content-type">>, <<"text/plain; charset=utf-8">>}
			], Value, Req);
    	{failure, _Reason} ->
			cowboy_req:reply(400, [], <<"Failed to put your key, sorry :(">>, Req)
	end.
