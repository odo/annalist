-module(annalist).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/annalist.test").
-endif.

-define(KEYSEPARATOR, <<":">>).
-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

% export for tests
-export([encode_key/2, encode_tags/1]).

-export([
	start/1,
	count/1,
	count/2,
	counts_with_labels/4,
	counts/4,
	leveldb_handle/0
]).

-type tags() :: [atom()].
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

start(LevelDBDir) ->
	application:set_env(annalist, level_db_dir, LevelDBDir),
	application:start(annalist).

% API
-spec start_link(list()) -> [{ok, pid()}].
start_link(ElevelDBDir) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [ElevelDBDir], []).
	
stop() ->
	gen_server:call(?SERVER, {stop}).

-spec count(tags()) -> ok.
count(Tags) ->
	gen_server:call(?SERVER, {count, Tags, calendar:universal_time()}).

-spec count(tags(), {{integer(), integer(), integer()}, {integer(), integer(), integer()}}) -> ok.
count(Tags, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:call(?SERVER, {count, Tags, Time}).


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

handle_call({count, Tags, Time}, _From, State) ->
	Res = count(Tags, Time, State#state.handle),
	{reply, Res, State};

handle_call({counts_with_labels, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counts_with_labels(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({counts, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counts(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({leveldb_handle}, _From, State) ->
	{reply, State#state.handle, State};

handle_call({stop}, _From, State) ->
  {stop, normal, stopped, State}.

handle_cast(_Msg, State) ->
	{noreply, State}.
	
handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.


% internal functions

counts(Tags, Scope, TimeStart, Steps, DBHandle) ->
	[V || {_, V} <- counts_with_labels(Tags, Scope, TimeStart, Steps, DBHandle)].

counts_with_labels(Tags, Scope, TimeStart, Steps, DBHandle) ->
	RangeFun = proplists:get_value(Scope, [
		{year, fun date_ranges:year_range/2},
		{month, fun date_ranges:month_range/2},
		{day, fun date_ranges:day_range/2},
		{hour, fun date_ranges:hour_range/2},
		{minute, fun date_ranges:minute_range/2},
		{second, fun date_ranges:second_range/2}
	]),
	TimeRange = RangeFun(TimeStart, Steps),
	TimeEnd = lists:last(TimeRange), 
	CountsSparse = [{Time, V}||{{_, Time}, V} <- counts_with_full_keys(Tags, list_to_binary(atom_to_list(Scope)), TimeStart, TimeEnd, DBHandle)],
	unsparse_range(CountsSparse, TimeRange, 0).

counts_with_full_keys(Tags, Scope, TimeStart, TimeEnd, DBHandle) ->
	Bucket = encode_tags(Tags),
	KeyStart = encode_key(Scope, TimeStart),
	KeyEnd = encode_key(Scope, TimeEnd),
	[{decode_key(K), V} || {K, V} <- uplevel:range(Bucket, KeyStart, KeyEnd, DBHandle)].

count(Tags, {{Y, Mo, D}, {H, Mi, S}}, DBHandle) ->
	record_tag(Tags, {Y, Mo, D, H, Mi, S}, 	<<"second">>, 	DBHandle),
	record_tag(Tags, {Y, Mo, D, H, Mi}, 	<<"minute">>, 	DBHandle),
	record_tag(Tags, {Y, Mo, D, H},	 		<<"hour">>, 	DBHandle),
	record_tag(Tags, {Y, Mo, D},		 	<<"day">>, 		DBHandle),
	record_tag(Tags, {Y, Mo},				<<"month">>, 	DBHandle),
	record_tag(Tags, {Y},			 		<<"year">>, 	DBHandle),
	record_tag(Tags, {},				 	<<"total">>, 	DBHandle).

record_tag([], _, _, _) -> ok;

record_tag(Tags, Time, Scope, DBHandle) ->
	increment(Tags, 	 Time, Scope, DBHandle),
	[_| TagsRest] = lists:reverse(Tags),
	record_tag(lists:reverse(TagsRest), Time, Scope, DBHandle).

increment(Tags, Time, Scope, DBHandle) ->
	Bucket = encode_tags(Tags),
	Key = encode_key(Scope, Time),
	Count =
	case uplevel:get(Bucket, Key, DBHandle, []) of
		not_found -> 0;
		{Key, Number} -> Number
	end,
	uplevel:put(Bucket, Key, Count + 1, DBHandle, [{put_options, [sync, true]}]).	

encode_tags(Tags) ->
	encode_tags(Tags, <<>>).

encode_tags([], TagsBinary) ->
	TagsBinary;

encode_tags([T | Tags], TagsBinary) ->
	TagBinary = list_to_binary(atom_to_list(T)),
	TagsBinaryNew = <<TagsBinary/binary, <<"/">>/binary, TagBinary/binary>>,
	encode_tags(Tags, TagsBinaryNew).

encode_key(Scope, Time) ->
	TimeEnc = encode_time(Time),
	<< Scope/binary, ?KEYSEPARATOR/binary, TimeEnc/binary>>.

decode_key(Key) ->
	[Scope, TimeEnc] = binary:split(Key, ?KEYSEPARATOR),
	{Scope, decode_time(TimeEnc)}.

encode_time({}) ->
	<<>>;

encode_time({Year}) ->
	<<Year:16>>;

encode_time({Year, Month}) ->
	<<Year:16, Month:8>>;

encode_time({Year, Month, Day}) ->
	<<Year:16, Month:8, Day:8>>;

encode_time({Year, Month, Day, Hour}) ->
	<<Year:16, Month:8, Day:8, Hour:8>>;

encode_time({Year, Month, Day, Hour, Minute}) ->
	<<Year:16, Month:8, Day:8, Hour:8, Minute:8>>;

encode_time({Year, Month, Day, Hour, Minute, Second}) ->
	<<Year:16, Month:8, Day:8, Hour:8, Minute:8, Second:8>>.

decode_time(<<>>) ->
	{};

decode_time(<<Year:16>>) ->
	{Year};

decode_time(<<Year:16, Month:8>>) ->
	{Year, Month};

decode_time(<<Year:16, Month:8, Day:8>>) ->
	{Year, Month, Day};

decode_time(<<Year:16, Month:8, Day:8, Hour:8>>) ->
	{Year, Month, Day, Hour};

decode_time(<<Year:16, Month:8, Day:8, Hour:8, Minute:8>>) ->
	{Year, Month, Day, Hour, Minute};

decode_time(<<Year:16, Month:8, Day:8, Hour:8, Minute:8, Second:8>>) ->
	{Year, Month, Day, Hour, Minute, Second}.

unsparse_range(ListSparse, Keys, Default) ->
	lists:reverse(unsparse_range(ListSparse, Keys, Default, [])).

unsparse_range(ListSparse = [{KSparse, VSparse}|RestSparse], [Key | Keys], Default, Acc) ->
	{AccNew, SparseNew} = case KSparse =:= Key of
		true ->
			{[{KSparse, VSparse} | Acc], RestSparse};
		false ->
			{[{Key, Default} | Acc], ListSparse}
	end,
	unsparse_range(SparseNew, Keys, Default, AccNew);

unsparse_range([], Keys, Default, Acc) ->
	Rest = [{K, Default}|| K <- Keys],
	lists:reverse(Rest) ++ Acc;

unsparse_range(_, [], _Default, Acc) ->
	Acc.


%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

store_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
      	{"test time encoding", fun test_time_encoding/0},
      	{"test tag encoding", fun test_tag_encoding/0},
      	{"test counting", fun test_counting/0},
      	{"test counting with time combinations", fun test_counting_time_combi/0},
      	{"test counting with tag combinations", fun test_counting_tag_combi/0},
      	{"test getting counts", fun test_counts/0},
      	{"test unsparsing", fun test_unsparse_range/0}
		]}
	].

test_setup() ->
	os:cmd("rm -rf " ++ ?TESTDB),
	start_link(?TESTDB).

test_teardown(_) ->
	stop().

test_time_encoding()  ->
	Times = [
	{},
	{2001},
	{2001, 12},
	{2001, 12, 31},
	{2001, 12, 31, 23},
	{2001, 12, 31, 23, 59},
	{2001, 12, 31, 23, 59, 59}
	],
	[?assertEqual(Time, decode_time(encode_time(Time))) || Time <- Times].

test_tag_encoding() ->
	?assertEqual(<<"/first/second/third">>, encode_tags([first, second, third])).

test_counting() ->
	Handle = leveldb_handle(),
	count([one, two, three], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>, {2012, 2, 6, 17}},
		{<<"day">>, {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>, {2012}},
		{<<"total">>, {}}
	],
	Keys = [encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two, three]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two		]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one			]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual(not_found, uplevel:get(encode_tags([two, three]), Key, Handle, [])) || Key <- Keys].

test_counting_time_combi() ->
	Handle = leveldb_handle(),
	count([one, two], {{2012,2,6},{17,22,5}}),
	count([one, two], {{2012,2,6},{18,16,5}}),
	% should be recorded single
	ScopeTimesSingle = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"second">>, {2012, 2, 6, 18, 16, 5}},
		{<<"minute">>, {2012, 2, 6, 18, 16}},
		{<<"hour">>,   {2012, 2, 6, 18}}
	],
	KeysSingle = [encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesSingle],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two]), Key, Handle, [])) || Key <- KeysSingle],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one	 ]), Key, Handle, [])) || Key <- KeysSingle],
	% should be recorded double
	ScopeTimesDouble = [
		{<<"day">>,   {2012, 2, 6}},
		{<<"month">>, {2012, 2}},
		{<<"year">>,  {2012}},
		{<<"total">>, {}}
	],
	KeysDouble = [encode_key(Scope, Time) || {Scope, Time} <- ScopeTimesDouble],
	[?assertEqual({Key, 2}, uplevel:get(encode_tags([one, two]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(encode_tags([one, two]), Key, Handle, [])) || Key <- KeysDouble],
	[?assertEqual({Key, 2}, uplevel:get(encode_tags([one	 ]), Key, Handle, [])) || Key <- KeysDouble].

test_counting_tag_combi() ->
	Handle = leveldb_handle(),
	count([one, two, three1], {{2012,2,6},{17,22,5}}),
	count([one, two, three2], {{2012,2,6},{17,22,5}}),
	ScopeTimes = [
		{<<"second">>, {2012, 2, 6, 17, 22, 5}},
		{<<"minute">>, {2012, 2, 6, 17, 22}},
		{<<"hour">>,   {2012, 2, 6, 17}},
		{<<"day">>,    {2012, 2, 6}},
		{<<"month">>,  {2012, 2}},
		{<<"year">>,   {2012}},
		{<<"total">>,  {}}
	],
	Keys = [encode_key(Scope, Time) || {Scope, Time} <- ScopeTimes],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two, three1]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 1}, uplevel:get(encode_tags([one, two, three2]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(encode_tags([one, two		]), Key, Handle, [])) || Key <- Keys],
	[?assertEqual({Key, 2}, uplevel:get(encode_tags([one			]), Key, Handle, [])) || Key <- Keys].
			
test_unsparse_range() ->
	Sparse = [{1, one}, {2, two}, {4, four}],
	?assertEqual([{1, one}, {2, two}], unsparse_range([{1, one}, {2, two}], [1, 2], undefined)),
	?assertEqual([{1, one}, {2, two}], unsparse_range([{1, one}, {2, two}, {2, three}], [1, 2], undefined)),
	?assertEqual([{1, one}, {2, two}, {3, undefined}, {4, four}], unsparse_range(Sparse, [1, 2, 3, 4], undefined)),
	?assertEqual([{1, one}, {2, two}, {3, undefined}, {4, four}, {5, undefined}], unsparse_range(Sparse, [1, 2, 3, 4, 5], undefined)),
	?assertEqual([{0, undefined}, {1, one}, {2, two}, {3, undefined}, {4, four}], unsparse_range(Sparse, [0, 1, 2, 3, 4], undefined)),
	?assertEqual([{-1, undefined}, {0, undefined}, {1, one}, {2, two}, {3, undefined}, {4, four}, {5, undefined}, {6, undefined}], unsparse_range(Sparse, [-1, 0, 1, 2, 3, 4, 5, 6], undefined)),
	?assertEqual([{0, undefined}, {1, undefined}, {2, undefined}, {3, undefined}], unsparse_range([], [0, 1, 2, 3], undefined)).

test_counts() ->
	count([one, two, three], {{2012,2,6},{17,22,5}}),
	?assertEqual([{{2012,2,6,17,22,4},0},
                 {{2012,2,6,17,22,5},1},
                 {{2012,2,6,17,22,6},0}],
                 counts_with_labels([one], second, {2012,2,6,17,22,4}, 3)),
	?assertEqual([0, 1, 0], counts([one], second, {2012,2,6,17,22,4}, 3)).

-endif.
