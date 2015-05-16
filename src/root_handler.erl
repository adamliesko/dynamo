%% Description %%
%% An Cowboy path handler, can be thought of as a C in the MVC structure. We define the acceptable incoming method and
%% specify API replies in here.
-module(root_handler).

-export([init/2]).

%% check http/rest method type and calls appropriate private fnc
init(Req, Opts) ->
	Req2 = case cowboy_req:method(Req) of
		<<"GET">> ->
			process_get_request(Req);
		<<"PUT">> ->
			process_put_request(Req);
		(_) ->
			cowboy_req:reply(405, Req)
	end,
	{ok, Req2, Opts}.

%% procesess GET request for a key
%% return not_found in case the key was not found in the dynamo cluster (conditions defined by setup)
%% return failure in case of missing params, or something worse, we pretend that your param is always missing
%% 					in case of a failure
%% on a s
process_get_request(Req) ->
	#{key := Key} = cowboy_req:match_qs([key], Req),
	case director:get(Key) of
		{ok,{ok,not_found}} -> cowboy_req:reply(400, [], <<"Key not found.">>, Req);
		{ok, 	    {failure, _Reason}} -> cowboy_req:reply(400, [], <<"Key not found.">>, Req);
	    {ok, {_Context,	     Values}} ->
						{Context,_Value}=Values,
error_logger:info_msg("~nThis is the replyCONTEXT: ~p VALS:~p~n", [Context, Values]),
R= io_lib:format("~p",[Values]),
lists:flatten(R),
						%%Response = lists:concat([context_,Context,Value]),
		    	cowboy_req:reply(200, [
					{<<"content-type">>, <<"text/plain; charset=utf-8">>}
				], R, Req);

	    {failure, _Reason} ->
	    	cowboy_req:reply(400, [], <<"Missing key parameter.">>, Req)
 	 end.

process_put_request(Req) ->
	{ok, Params, _Req2}  = cowboy_req:body_qs(Req),
	Key = proplists:get_value(<<"key">>, Params),
	Value = proplists:get_value(<<"value">>, Params),
	TContext =  proplists:get_value(<<"context">>, Params),
	Context = case TContext of

					undefined -> [];
					_ ->
					X=binary_to_list(TContext),
					error_logger:info_msg("~nTHIS IS THE CONTEXT: ~p~n", [X]),
          					X
	end,
	case director:put(Key, Context, Value) of
		{failure, _Reason} ->
		cowboy_req:reply(400, [], <<"Failed to put your key, sorry :(">>, Req);
		{ok, _N} ->
			cowboy_req:reply(200, [
				{<<"content-type">>, <<"text/plain; charset=utf-8">>}
			], Value, Req)

	end.
