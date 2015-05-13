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
		{ok,{ok,not_found}} -> cowboy_req:reply(400, [], <<"Key not found.">>, Req);
	    {ok, {_Context, Values}} ->
						{_,Value}=Values,
						%%Response = lists:concat([context_,Context,value_, Value]),
		    	cowboy_req:reply(200, [
					{<<"content-type">>, <<"text/plain; charset=utf-8">>}
				], Value, Req);

	    {failure, _Reason} ->
	    	cowboy_req:reply(400, [], <<"Missing key parameter.">>, Req)
 	 end.

process_post_request(Req) ->
	{ok, Params, _Req2}  = cowboy_req:body_qs(Req),
	Key = proplists:get_value(<<"key">>, Params),
	Value = proplists:get_value(<<"value">>, Params),
	TContext = proplists:get_value(<<"context">>, Params),
	IsInt = is_integer_p(TContext),
	Context = if IsInt -> list_to_integer(binary_to_list(TContext));
		true -> []
	end,
	case director:put(Key, Context, Value) of
		{ok, _N} ->
			cowboy_req:reply(200, [
				{<<"content-type">>, <<"text/plain; charset=utf-8">>}
			], Value, Req);
    	{failure, _Reason} ->
			cowboy_req:reply(400, [], <<"Failed to put your key, sorry :(">>, Req)
	end.

%% is_integer taken from http://stackoverflow.com/questions/4536046/test-if-a-string-is-a-number
is_integer_p(S) ->
    try
        _ = list_to_integer(binary_to_list(S)),
        true
    catch error:badarg ->
        false
    end.
