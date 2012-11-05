
-module(annalist_handler).
-export([init/3, handle/2, terminate/2]).
-record(state, {context}).

init({tcp, http}, Req, Opts) ->
	Context = proplists:get_value(context, Opts),
    {ok, Req, #state{context = Context}}.

handle(Req, State) ->
	{CallbackRaw, _} = cowboy_req:qs_val(<<"callback">>, Req), 
	Callback =
	case CallbackRaw of
		undefined ->
			undefined;
		_ ->
			binary_to_list(CallbackRaw)
	end, 
	Context = State#state.context,
	{CountRaw, _} = cowboy_req:binding(count, Req),
	Count = binary_to_integer(CountRaw),
	{TagsRaw, _} = cowboy_req:binding(tags, Req),
	Tags = [T || T <- binary:split(TagsRaw, <<" ">>)],
	{Year, _} 	= cowboy_req:binding(year, Req),
	{Month, _} 	= cowboy_req:binding(month, Req),
	{Day, _} 	= cowboy_req:binding(day, Req),
	{Hour, _} 	= cowboy_req:binding(hour, Req),
	{Minute, _} = cowboy_req:binding(minute, Req),
	{Second, _} = cowboy_req:binding(second, Req),
	Times = list_to_tuple(
		lists:filter(
			fun(E) -> E =/= undefined end,
			[binary_to_integer(E)||E<-[Year, Month, Day, Hour, Minute, Second]]
		)
	),
	Counts = annalist_api_server:counts(Tags, Context, Times, Count),
	JSON = json:to_binary(Counts, Callback),
	ContentType =
	case Callback of
		undefined -> 	<<"application/json">>;
		_ -> 			<<"application/javascript">>
	end,
    Req2 = cowboy_req:set_resp_header(<<"Content-Type">>, ContentType, Req),
    {ok, Req3} = cowboy_req:reply(200, [], JSON, Req2),
    {ok, Req3, State}.

terminate(_Req, _State) ->
    ok.

binary_to_integer(undefined) ->
	undefined;

binary_to_integer(Binary) ->
	list_to_integer(binary_to_list(Binary)).