
-module(annalist_handler).
-export([init/3, handle/2, terminate/2]).
-record(state, {context}).

init({tcp, http}, Req, Opts) ->
	Context = proplists:get_value(context, Opts),
    {ok, Req, #state{context = Context}}.

handle(Req, State) ->
	{CallbackRaw, _} = cowboy_req:qs_val(<<"callback">>, Req), 
	Callback = binary_to_list(CallbackRaw), 
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
	Counts =
	case Context of
		total ->
			annalist_api_server:counts(Tags, Context);
		year ->
			annalist_api_server:counts(Tags, Context, 
				{
					binary_to_integer(Year)
				}
				, Count);
		month ->
			annalist_api_server:counts(Tags, Context, 
				{
					binary_to_integer(Year),
					binary_to_integer(Month)
				}
				, Count);
		day ->
			annalist_api_server:counts(Tags, Context, 
				{
					binary_to_integer(Year),
					binary_to_integer(Month),
					binary_to_integer(Day)
				}
				, Count);
		hour ->
			annalist_api_server:counts(Tags, Context, 
				{
					binary_to_integer(Year),
					binary_to_integer(Month),
					binary_to_integer(Day),
					binary_to_integer(Hour)
				}
				, Count);
		minute ->
			annalist_api_server:counts(Tags, Context, 
				{
					binary_to_integer(Year),
					binary_to_integer(Month),
					binary_to_integer(Day),
					binary_to_integer(Hour),
					binary_to_integer(Minute)
				}
				, Count);
		second ->
			annalist_api_server:counts(Tags, second, 
				{
					binary_to_integer(Year),
					binary_to_integer(Month),
					binary_to_integer(Day),
					binary_to_integer(Hour),
					binary_to_integer(Minute),
					binary_to_integer(Second)
				}
				, Count);
		Other ->
			throw({error, {unknown_context, Other}})
	end,
	JSON = json:to_binary(Counts, Callback),
	ContentType =
	case Callback of
		undefined -> 	"application/json";
		_ -> 			"application/javascript"
	end,
    Req2 = cowboy_req:set_resp_header('Content-Type', ContentType, Req),
    Req3 = cowboy_req:reply(200, [], JSON, Req2),
    {ok, Req3, State}.

terminate(_Req, _State) ->
    ok.

binary_to_integer(undefined) ->
	undefined;

binary_to_integer(Binary) ->
	list_to_integer(binary_to_list(Binary)).