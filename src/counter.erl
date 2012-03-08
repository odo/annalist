-module(counter).

% This module provides functions for counting single events and
% requesting time series of accumulated numbers of events.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([encode_key/2, encode_tags/1]).
-endif.

-export([
	count/3,
	counts/5,
	counts_with_labels/5
]).


-define(KEYSEPARATOR, <<":">>).

counts(Tags, Scope, TimeStart, Steps, DBHandle) ->
	[V || {_, V} <- counts_with_labels(Tags, Scope, TimeStart, Steps, DBHandle)].

counts_with_labels(Tags, Scope, TimeStart, Steps, DBHandle) ->
	RangeFun = proplists:get_value(Scope, [
		{year,		fun date_ranges:year_range/2},
		{month, 	fun date_ranges:month_range/2},
		{day,		fun date_ranges:day_range/2},
		{hour, 		fun date_ranges:hour_range/2},
		{minute, 	fun date_ranges:minute_range/2},
		{second, 	fun date_ranges:second_range/2}
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
	uplevel:put(Bucket, Key, Count + 1, DBHandle, []).	

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

time_encoding_test()  ->
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

tag_encoding_test() ->
	?assertEqual(<<"/first/second/third">>, encode_tags([first, second, third])).

unsparse_range_test() ->
	Sparse = [{1, one}, {2, two}, {4, four}],
	?assertEqual([{1, one}, {2, two}], unsparse_range([{1, one}, {2, two}], [1, 2], undefined)),
	?assertEqual([{1, one}, {2, two}], unsparse_range([{1, one}, {2, two}, {2, three}], [1, 2], undefined)),
	?assertEqual([{1, one}, {2, two}, {3, undefined}, {4, four}], unsparse_range(Sparse, [1, 2, 3, 4], undefined)),
	?assertEqual([{1, one}, {2, two}, {3, undefined}, {4, four}, {5, undefined}], unsparse_range(Sparse, [1, 2, 3, 4, 5], undefined)),
	?assertEqual([{0, undefined}, {1, one}, {2, two}, {3, undefined}, {4, four}], unsparse_range(Sparse, [0, 1, 2, 3, 4], undefined)),
	?assertEqual([{-1, undefined}, {0, undefined}, {1, one}, {2, two}, {3, undefined}, {4, four}, {5, undefined}, {6, undefined}], unsparse_range(Sparse, [-1, 0, 1, 2, 3, 4, 5, 6], undefined)),
	?assertEqual([{0, undefined}, {1, undefined}, {2, undefined}, {3, undefined}], unsparse_range([], [0, 1, 2, 3], undefined)).

-endif.
