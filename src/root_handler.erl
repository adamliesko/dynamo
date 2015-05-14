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
		<<"POST">> ->
			process_post_request(Req);
		(_) ->
			cowboy_req:reply(405, Req)
	end,
	{ok, Req2, Opts}.

%% procesess GET request for a key
%% key - specifies key/value tuple to be looked up
%% returns not_found in case the key was not found in the dynamo cluster (conditions defined by setup)
%% returns failure in case of missing params, or something worse
%% on a sucessful key lookup returns the key value ( maybe we will add Context)
process_get_request(Req) ->
	#{key := Key} = cowboy_req:match_qs([key], Req),
	case director:get(Key) of
		{ok,{ok,not_found}} -> cowboy_req:reply(400, [], <<"Key not found.">>, Req);
		{ok, 	    {failure, _Reason}} -> cowboy_req:reply(400, [], <<"Key not found, maybe error.">>, Req);
	    {ok, {_Context, Values}} ->
						{_,Value}=Values,
						%%Response = lists:concat([context_,Context,value_, Value]),
		    	cowboy_req:reply(200, [
					{<<"content-type">>, <<"text/plain; charset=utf-8">>}
				], Value, Req);

	    {failure, _Reason} ->
	    	cowboy_req:reply(400, [], <<"Missing key parameter.">>, Req)
 	 end.

%% procesess POST request for a key
%% key, value, context = params
%% returns failure in case of unsuccessful key put
%% on a sucessful key lookup returns the posted key value ( possibly changing to the number of saved nodes)

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
		{failure, _Reason} ->
		cowboy_req:reply(400, [], <<"Failed to put your key, sorry :(">>, Req);
		{ok, _N} ->
			cowboy_req:reply(200, [
				{<<"content-type">>, <<"text/plain; charset=utf-8">>}
			], Value, Req)

	end.

%% is_integer taken from http://stackoverflow.com/questions/4536046/test-if-a-string-is-a-number
%% checks whether argument is an integer
is_integer_p(S) ->
    try
        _ = list_to_integer(binary_to_list(S)),
        true
    catch error:badarg ->
        false
    end.
