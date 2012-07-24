-module(recorder).

-include("quantile_estimator.hrl").

-export([
	record/6
]).

-define(
	INVARIANT,
	quantile_estimator:f_targeted([{0.01, 0.005}, {0.05, 0.005}, {0.5, 0.025}, {0.95, 0.005}, {0.99, 0.005}])
).

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
	Bucket = annalist_utils:encode_bucket(Tags),
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
