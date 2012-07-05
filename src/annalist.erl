-module(annalist).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/annalist.test").
-endif.

-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

-export([
	start/5,
	queue_length/0,
	beacon/0,
	count_sync/1, count_sync/2,
	count/1, count/2,
	count_sparse/2, count_sparse/3,
	counts_with_labels/4,
	counts/4,
	leveldb_handle/0
]).

-type tags() :: [binary()].
-type scope() :: year | month | day | hour | minute | second.
-type time() :: {integer(), integer(), integer(), integer(), integer(), integer()} |
				{integer(), integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer()} | 
				{integer(), integer()} | 
				{integer()}. 

% callbacks
-export ([init/1, stop/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/1]).

% start the app

start(LevelDBDir, Host, Port, OutsidePort, Password) ->
	application:set_env(annalist, level_db_dir, LevelDBDir),
	application:set_env(annalist, host, 		Host),
	application:set_env(annalist, port, 		Port),
	application:set_env(annalist, outside_port, OutsidePort),
	application:set_env(annalist, password, 	Password),
	application:start(annalist).

% API

queue_length() ->
	{_, Length}  = erlang:process_info(whereis(?SERVER), message_queue_len),
	Length.

-spec start_link(list()) -> [{ok, pid()}].
start_link(ElevelDBDir) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [ElevelDBDir], []).
	
stop() ->
	gen_server:call(?SERVER, {stop}).

-spec beacon() -> ok.
beacon() ->
	gen_server:call(?SERVER, {beacon}).

-spec count_sync(tags()) -> ok.
count_sync(Tags) ->
	gen_server:call(?SERVER, {count, Tags, calendar:universal_time()}).

-spec count_sync(tags(), {{integer(), integer(), integer()}, {integer(), integer(), integer()}}) -> ok.
count_sync(Tags, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:call(?SERVER, {count, Tags, Time}).

-spec count(tags()) -> ok.
count(Tags) ->
	gen_server:cast(?SERVER, {count, Tags, calendar:universal_time()}).

-spec count(tags(), {{integer(), integer(), integer()}, {integer(), integer(), integer()}}) -> ok.
count(Tags, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:cast(?SERVER, {count, Tags, Time}).

-spec count_sparse(tags(), non_neg_integer()) -> ok.
count_sparse(Tags, SparsenessFactor) ->
	count_sparse(Tags, calendar:universal_time(), SparsenessFactor).

-spec count_sparse(tags(), {{integer(), integer(), integer()}, {integer(), integer(), integer()}}, non_neg_integer()) -> ok.
count_sparse(Tags, Time = {{_, _, _}, {_, _, _}}, SparsenessFactor) ->
	case random:uniform() < 1 / SparsenessFactor of
		true ->
			gen_server:cast(?SERVER, {count_inc, SparsenessFactor, Tags, Time});
		false ->
			nothing
	end.


-spec counts_with_labels(tags(), scope(), time(), integer()) -> ok.
counts_with_labels(Tags, year, TimeStart = {_}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, year, TimeStart, Steps});

counts_with_labels(Tags, month, TimeStart = {_, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, month, TimeStart, Steps});

counts_with_labels(Tags, day, TimeStart = {_, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, day, TimeStart, Steps});

counts_with_labels(Tags, hour, TimeStart = {_, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, hour, TimeStart, Steps});

counts_with_labels(Tags, minute, TimeStart = {_, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, minute, TimeStart, Steps});

counts_with_labels(Tags, second, TimeStart = {_, _, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, second, TimeStart, Steps}).


-spec counts(tags(), scope(), time(), integer()) -> ok.
counts(Tags, year, TimeStart = {_}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, year, TimeStart, Steps});

counts(Tags, month, TimeStart = {_, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, month, TimeStart, Steps});

counts(Tags, day, TimeStart = {_, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, day, TimeStart, Steps});

counts(Tags, hour, TimeStart = {_, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, hour, TimeStart, Steps});

counts(Tags, minute, TimeStart = {_, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, minute, TimeStart, Steps});

counts(Tags, second, TimeStart = {_, _, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, second, TimeStart, Steps}).


leveldb_handle() ->
	gen_server:call(?SERVER, {leveldb_handle}).

% gen_server callbacks

init([ElevelDBDir]) ->
	{ok, #state{handle = uplevel:handle(ElevelDBDir)}}.

handle_call({beacon}, _From, State) ->
	{reply, ok, State};

handle_call({count, Tags, Time}, _From, State) ->
	Res = counter:count(1, Tags, Time, State#state.handle),
	{reply, Res, State};

handle_call({count_inc, Increment, Tags, Time}, _From, State) ->
	Res = counter:count(Increment, Tags, Time, State#state.handle),
	{reply, Res, State};

handle_call({counts_with_labels, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts_with_labels(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({counts, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({leveldb_handle}, _From, State) ->
	{reply, State#state.handle, State};

handle_call({stop}, _From, State) ->
  {stop, normal, stopped, State}.

handle_cast({count, Tags, Time}, State) ->
	counter:count(1, Tags, Time, State#state.handle),
	{noreply, State};
	
handle_cast({count_inc, Increment, Tags, Time}, State) ->
	counter:count(Increment, Tags, Time, State#state.handle),
	{noreply, State}.
	
handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

store_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
      	{"test counting", fun test_counting/0},
      	{"test sparse counting", fun test_sparse_counting/0},
      	{"test counting with time combinations", fun test_counting_time_combi/0},
      	{"test counting with tag combinations", fun test_counting_tag_combi/0},
      	{"test getting counts", fun test_counts/0}
		]}
	].

test_setup() ->
	random:seed(now()),
	os:cmd("rm -rf " ++ ?TESTDB),
	start_link(?TESTDB).

test_teardown(_) ->
	stop().

test_counting() ->
	Handle = leveldb_handle(),
	count_sync([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>, {2012, 2, 6, 17}},
		{<<"day">>, {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>, {2012}},
		{<<"total">>, {}}
	],
	Keys = [counter:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>, <<"three">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>				]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>						]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual(not_found, uplevel:get(counter:encode_bucket([<<"two">>, <<"three">>			]), Key, Handle, [])) || Key <- Keys].

test_sparse_counting() ->
	Handle = leveldb_handle(),
	Samples = 10000,
	lists:map(
		fun(_) ->
			count_sparse([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}, 20)
		end,
		lists:seq(1, Samples)
	),
	% wait for the stats to be written
	beacon(),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>, {2012, 2, 6, 17}},
		{<<"day">>, {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>, {2012}},
		{<<"total">>, {}}
	],
	Keys = [counter:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	lists:map(
		fun(Key) ->
			{Key, SamplesRec} = uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>, <<"three">>]), Key, Handle, []), 
			?assert(SamplesRec + SamplesRec * 0.1 > Samples), 
			?assert(SamplesRec - SamplesRec * 0.1 < Samples) 
		end,
		Keys
	).

test_counting_time_combi() ->
	Handle = leveldb_handle(),
	count_sync([<<"one">>, <<"two">>], {{2012,2,6},{17,22,5}}),
	count_sync([<<"one">>, <<"two">>], {{2012,2,6},{18,16,5}}),
	% should be recorded single
	ScopeTimesSingle = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"second">>, {2012, 2, 6, 18, 16, 5}},
		{<<"minute">>, {2012, 2, 6, 18, 16}},
		{<<"hour">>,   {2012, 2, 6, 18}}
	],
	KeysSingle = [counter:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesSingle],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>	 ]), Key, Handle, [])) || Key <- KeysSingle],
	% should be recorded double
	ScopeTimesDouble = [
		{<<"day">>,   {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>,  {2012}},
		{<<"total">>, {}}
	],
	KeysDouble = [counter:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesDouble],
	[?assertEqual({Key, 2}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(counter:encode_bucket([<<"one">>	 ]), Key, Handle, [])) || Key <- KeysDouble].

test_counting_tag_combi() ->
	Handle = leveldb_handle(),
	count_sync([<<"one">>, <<"two">>, <<"three1">>], {{2012,2,6},{17,22,5}}),
	count_sync([<<"one">>, <<"two">>, <<"three2">>], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"day">>,    {2012, 2, 6}},
		{<<"month">>,  {2012, 2}},
		{<<"year">>,   {2012}},
		{<<"total">>,  {}}
	],
	Keys = [counter:encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>, <<"three1">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>, <<"three2">>]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(counter:encode_bucket([<<"one">>, <<"two">>		]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(counter:encode_bucket([<<"one">>			]), Key, Handle, [])) || Key <- Keys].
			
test_counts() ->
	count_sync([<<"one">>, <<"two">>, <<"three">>], {{2012,2,6},{17,22,5}}),
	?assertEqual([{{2012,2,6,17,22,4},0},
                 {{2012,2,6,17,22,5},1},
                 {{2012,2,6,17,22,6},0}],
                 counts_with_labels([<<"one">>], second, {2012,2,6,17,22,4}, 3)),
	?assertEqual([0, 1, 0], counts([<<"one">>], second, {2012,2,6,17,22,4}, 3)).

-endif.
