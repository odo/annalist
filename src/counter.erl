-module(counter).

% This module provides functions for counting single events and
% requesting time series of accumulated numbers of events.

-export([
	count/4,
	counts/5,
	counts_with_labels/5
]).

counts(Tags, Scope, TimeStart, Steps, DBHandle) ->
	[V || {_, V} <- counts_with_labels(Tags, Scope, TimeStart, Steps, DBHandle)].

counts_with_labels(Tags, total, {}, 1, DBHandle) ->
	Bucket = annalist_utils:encode_count_bucket(Tags),
	Key = annalist_utils:encode_key(<<"total">>, {}),
	Res = case uplevel:get(Bucket, Key, DBHandle, []) of
		not_found ->
			0;
		{_, Number} ->
			Number
	end,
	[{total, Res}];

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
	annalist_utils:unsparse_range(CountsSparse, TimeRange, 0).

counts_with_full_keys(Tags, Scope, TimeStart, TimeEnd, DBHandle) ->
	Bucket = annalist_utils:encode_count_bucket(Tags),
	KeyStart = annalist_utils:encode_key(Scope, TimeStart),
	KeyEnd = annalist_utils:encode_key(Scope, TimeEnd),
	[{annalist_utils:decode_key(K), V} || {K, V} <- uplevel:range(Bucket, KeyStart, KeyEnd, DBHandle)].

count(Increment, Tags, {{Y, Mo, D}, {H, Mi, S}}, DBHandle) ->
	record_tag(Increment, Tags, {Y, Mo, D, H, Mi, S},	<<"second">>, 	DBHandle),
	record_tag(Increment, Tags, {Y, Mo, D, H, Mi}, 		<<"minute">>, 	DBHandle),
	record_tag(Increment, Tags, {Y, Mo, D, H},	 		<<"hour">>, 	DBHandle),
	record_tag(Increment, Tags, {Y, Mo, D},		 		<<"day">>, 		DBHandle),
	record_tag(Increment, Tags, {Y, Mo},				<<"month">>, 	DBHandle),
	record_tag(Increment, Tags, {Y},			 		<<"year">>, 	DBHandle),
	record_tag(Increment, Tags, {},				 		<<"total">>, 	DBHandle).

record_tag(_, [], _, _, _) -> ok;

record_tag(Increment, Tags, Time, Scope, DBHandle) ->
	increment(Increment, Tags, Time, Scope, DBHandle),
	[_| TagsRest] = lists:reverse(Tags),
	record_tag(Increment, lists:reverse(TagsRest), Time, Scope, DBHandle).

increment(Increment, Tags, Time, Scope, DBHandle) ->
	Bucket = annalist_utils:encode_count_bucket(Tags),
	Key = annalist_utils:encode_key(Scope, Time),
	Count =
	case uplevel:get(Bucket, Key, DBHandle, []) of
		not_found -> 0;
		{Key, Number} -> Number
	end,
	uplevel:put(Bucket, Key, Count + Increment, DBHandle, []).	
