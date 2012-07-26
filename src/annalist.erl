-module(annalist).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/annalist.test").
-define(TESTHOST, "localhost").
-define(TESTPORT, 8887).
-endif.

-export([
	start/0
	, start/7
	, stop/0
]).

start(LevelDBDir, Host, Port, OutsidePort, Password, CompressThreshold, CompressFrequency) ->
	application:set_env(annalist, level_db_dir, LevelDBDir),
	application:set_env(annalist, host, 		Host),
	application:set_env(annalist, port, 		Port),
	application:set_env(annalist, outside_port, OutsidePort),
	application:set_env(annalist, password, 	Password),
	application:set_env(annalist, compress_threshold, 	CompressThreshold),
	application:set_env(annalist, compress_frequency, 	CompressFrequency),
	application:start(annalist).

start() ->
	application:start(annalist).

stop() ->
	application:stop(annalist).


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

store_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
      	{"test counting", fun test_counting/0}
      	, {"test sparse counting", fun test_sparse_counting/0}
      	, {"test counting with time combinations", fun test_counting_time_combi/0}
      	, {"test counting with tag combinations", fun test_counting_tag_combi/0}
      	, {"test getting counts", fun test_counts/0}
		]}
	].

test_setup() ->
	random:seed(now()),
	os:cmd("rm -rf " ++ ?TESTDB),
	start(?TESTDB, ?TESTHOST, ?TESTPORT, ?TESTPORT, undefined, 100, 100).

test_teardown(_) ->
	stop().

test_counting() ->
	Handle = annalist_api_server:leveldb_handle(),
	annalist_counter_server:count_sync([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>, {2012, 2, 6, 17}},
		{<<"day">>, {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>, {2012}},
		{<<"total">>, {}}
	],
	Keys = [annalist_utils:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>, <<"three">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>				]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>						]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual(not_found, uplevel:get(annalist_utils:encode_count_bucket([<<"two">>, <<"three">>			]), Key, Handle, [])) || Key <- Keys].

test_sparse_counting() ->
	Handle = annalist_api_server:leveldb_handle(),
	Samples = 10000,
	lists:map(
		fun(_) ->
			annalist_counter_server:count_sparse([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}, 20)
		end,
		lists:seq(1, Samples)
	),
	% wait for the stats to be written
	annalist_counter_server:beacon(),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>, {2012, 2, 6, 17}},
		{<<"day">>, {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>, {2012}},
		{<<"total">>, {}}
	],
	Keys = [annalist_utils:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	lists:map(
		fun(Key) ->
			{Key, SamplesRec} = uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>, <<"three">>]), Key, Handle, []), 
			?assert(SamplesRec + SamplesRec * 0.1 > Samples), 
			?assert(SamplesRec - SamplesRec * 0.1 < Samples) 
		end,
		Keys
	).

test_counting_time_combi() ->
	Handle = annalist_api_server:leveldb_handle(),
	annalist_counter_server:count_sync([<<"one">>, <<"two">>], {{2012,2,6},{17,22,5}}),
	annalist_counter_server:count_sync([<<"one">>, <<"two">>], {{2012,2,6},{18,16,5}}),
	% should be recorded single
	ScopeTimesSingle = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"second">>, {2012, 2, 6, 18, 16, 5}},
		{<<"minute">>, {2012, 2, 6, 18, 16}},
		{<<"hour">>,   {2012, 2, 6, 18}}
	],
	KeysSingle = [annalist_utils:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesSingle],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>	 ]), Key, Handle, [])) || Key <- KeysSingle],
	% should be recorded double
	ScopeTimesDouble = [
		{<<"day">>,   {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>,  {2012}},
		{<<"total">>, {}}
	],
	KeysDouble = [annalist_utils:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesDouble],
	[?assertEqual({Key, 2}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>	 ]), Key, Handle, [])) || Key <- KeysDouble].

test_counting_tag_combi() ->
	Handle = annalist_api_server:leveldb_handle(),
	annalist_counter_server:count_sync([<<"one">>, <<"two">>, <<"three1">>], {{2012,2,6},{17,22,5}}),
	annalist_counter_server:count_sync([<<"one">>, <<"two">>, <<"three2">>], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"day">>,    {2012, 2, 6}},
		{<<"month">>,  {2012, 2}},
		{<<"year">>,   {2012}},
		{<<"total">>,  {}}
	],
	Keys = [annalist_utils:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>, <<"three1">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>, <<"three2">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>, <<"two">>		]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(annalist_utils:encode_count_bucket([<<"one">>			]), Key, Handle, [])) || Key <- Keys].
			
test_counts() ->
	annalist_counter_server:count_sync([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}),
	?assertEqual([{{2012,2,6,17,22,4},0},
                 {{2012,2,6,17,22,5},1},
                 {{2012,2,6,17,22,6},0}],
                 annalist_api_server:counts_with_labels([<<"one">>], second, {2012,2,6,17,22,4}, 3)),
	?assertEqual([0, 1, 0], annalist_api_server:counts([<<"one">>], second, {2012,2,6,17,22,4}, 3)).

-endif.
