-module(annalist_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
	LevelDBDir = 	env_or_throw(level_db_dir),
	Port = 			env_or_throw(port),
	_Host = 		env_or_throw(host),
	_OutsidePort = 	env_or_throw(outside_port),
	CompressThreshold = 	env_or_throw(compress_threshold),
	CompressFrequency = 	env_or_throw(compress_frequency),
	% start http interface
	application:start(ranch),
	application:start(crypto),
	application:start(cowboy),
	Dispatch = [
    %% {Host, list({Path, Handler, Opts})}
    {'_', [
    		{[<<"annalist">>, <<"sparks">>], 		sparks_handler, []},
    		{[<<"annalist">>, <<"dashboard">>], 	dashboard_handler, []},
    		{[<<"annalist">>, <<"totals">>, 		tags], 														annalist_handler, [{context, total}]},
    		{[<<"annalist">>, <<"year_counts">>, 	tags, year, count], 									annalist_handler, [{context, year}]},
		    {[<<"annalist">>, <<"month_counts">>, 	tags, year, month, count], 								annalist_handler, [{context, month}]},
		    {[<<"annalist">>, <<"day_counts">>, 	tags, year, month, day, count], 						annalist_handler, [{context, day}]},
		    {[<<"annalist">>, <<"hour_counts">>, 	tags, year, month, day, hour, count], 					annalist_handler, [{context, hour}]},
		    {[<<"annalist">>, <<"minute_counts">>, 	tags, year, month, day, hour, minute, count], 			annalist_handler, [{context, minute}]},
		    {[<<"annalist">>, <<"second_counts">>, 	tags, year, month, day, hour, minute, second, count],	annalist_handler, [{context, second}]}
		]}
	],
	%% Name, NbAcceptors, TransOpts, ProtoOpts
	cowboy:start_http(annalist_listener, 100, [{port, Port}], [{env, [{dispatch, Dispatch}]}]),
	application:start(sasl),
	annalist_sup:start_link(LevelDBDir, CompressThreshold, CompressFrequency).


stop(_State) ->
    ok.

env_or_throw(Key) ->
	case proplists:get_value(Key, application:get_all_env(annalist)) of
		undefined ->
			throw({error, {atom_to_list(Key) ++ " must be configured in annalists' environment"}});
		Value ->
			Value
	end.
