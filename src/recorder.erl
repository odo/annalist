-module(recorder).

-include("quantile_estimator.hrl").

-export([
	record/6
	, quantiles_with_labels/6
	, quantiles/6
]).

-define(
	INVARIANT,
	quantile_estimator:f_targeted([{0.01, 0.005}, {0.05, 0.005}, {0.5, 0.025}, {0.95, 0.005}, {0.99, 0.005}])
).

quantiles(Tags, Scope, TimeStart, Steps, Quantile, DBHandle) ->
	[V || {_, V} <- quantiles_with_labels(Tags, Scope, TimeStart, Steps, Quantile, DBHandle)].

quantiles_with_labels(Tags, total, {}, 1, Quantile, DBHandle) ->
	Bucket = annalist_utils:encode_record_bucket(Tags),
	Key = annalist_utils:encode_key(<<"total">>, {}),
	Res = case uplevel:get(Bucket, Key, DBHandle, []) of
		not_found ->
			undefined;
		{_, Stats} ->
			quantile_estimator:quantile(Quantile, ?INVARIANT, Stats)
	end,
	[{total, Res}];

quantiles_with_labels(Tags, Scope, TimeStart, Steps, Quantile, DBHandle) ->
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
	CountsSparse = [{Time, quantile_estimator:quantile(Quantile, ?INVARIANT, Stats)}||{{_, Time}, Stats} <- quantiles_with_full_keys(Tags, list_to_binary(atom_to_list(Scope)), TimeStart, TimeEnd, DBHandle)],
	annalist_utils:unsparse_range(CountsSparse, TimeRange, undefined).

quantiles_with_full_keys(Tags, Scope, TimeStart, TimeEnd, DBHandle) ->
	Bucket = annalist_utils:encode_record_bucket(Tags),
	KeyStart = annalist_utils:encode_key(Scope, TimeStart),
	KeyEnd = annalist_utils:encode_key(Scope, TimeEnd),
	[{annalist_utils:decode_key(K), V} || {K, V} <- uplevel:range(Bucket, KeyStart, KeyEnd, DBHandle)].

record(Value, Tags, {{Y, Mo, D}, {H, Mi, S}}, DBHandle, CompressThreshold, CompressFrequency) ->
	record_tag(Value, Tags, {Y, Mo, D, H, Mi, S},	<<"second">>, 	DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {Y, Mo, D, H, Mi}, 		<<"minute">>, 	DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {Y, Mo, D, H},	 		<<"hour">>, 	DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {Y, Mo, D},		 		<<"day">>, 		DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {Y, Mo},				<<"month">>, 	DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {Y},			 		<<"year">>, 	DBHandle, CompressThreshold, CompressFrequency),
	record_tag(Value, Tags, {},				 		<<"total">>, 	DBHandle, CompressThreshold, CompressFrequency).

record_tag(_, [], _, _, _, _, _) -> ok;

record_tag(Value, Tags, Time, Scope, DBHandle, CompressThreshold, CompressFrequency) ->
	record_tag_value(Value, Tags, Time, Scope, DBHandle, CompressThreshold, CompressFrequency),
	[_| TagsRest] = lists:reverse(Tags),
	record_tag(Value, lists:reverse(TagsRest), Time, Scope, DBHandle, CompressThreshold, CompressFrequency).

record_tag_value(Value, Tags, Time, Scope, DBHandle, CompressThreshold, CompressFrequency) ->
	Bucket = annalist_utils:encode_record_bucket(Tags),
	Key = annalist_utils:encode_key(Scope, Time),
	Estimate =
	case uplevel:get(Bucket, Key, DBHandle, []) of
		not_found ->
			quantile_estimator:new();
		{_Key, EstimateRead} ->
			EstimateRead
	end,
	EstimateUpdated = quantile_estimator:insert(Value, ?INVARIANT, Estimate),
	EstimatePut = 
	case EstimateUpdated#quantile_estimator.data_count > CompressThreshold andalso
		 EstimateUpdated#quantile_estimator.inserts_since_compression > CompressFrequency of
		true  ->
			quantile_estimator:compress(?INVARIANT, EstimateUpdated);
		false ->
			EstimateUpdated
	end,
	ok = uplevel:put(Bucket, Key, EstimatePut, DBHandle, []).
