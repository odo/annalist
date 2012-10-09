-module(annalist_error_handler).

-behaviour(gen_event).

-export([
	add_handler/0, add_handler/1,
	delete_handler/0
]).

-export([
	init/1,
	handle_event/2,
	handle_call/2,
	handle_info/2,
	code_change/3,
	terminate/2
]).

init(Keys) ->
    {ok, [{keys, Keys}]}.

terminate(_Args, _State) ->
	ok.

add_handler() ->
	error_logger:add_report_handler(?MODULE, [<<"error">>]).

add_handler(Keys) ->
	error_logger:add_report_handler(?MODULE, Keys).

delete_handler() ->
	error_logger:delete_report_handler(?MODULE, []).

handle_event({error, _, _}, State = [{keys, Keys}]) ->
	annalist:count(Keys),
	{ok, State};

handle_event(_, State) ->
	{ok, State}.

handle_call(_Request, _State) ->
    {error, not_implemented}.

handle_info(_Msg, _State) ->
    {error, not_implemented}.

code_change(_OldVsn, _State, _Extra) ->
    {error, not_implemented}.
