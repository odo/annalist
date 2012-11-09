-module(annalist_utils).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([
	  encode_count_bucket/1
	, encode_record_bucket/1
	, encode_time/1
	, decode_time/1
	, encode_key/2
	, decode_key/1
	, unsparse_range/3
]).

-define(KEYSEPARATOR, <<"/$annalist_keysep">>).

encode_count_bucket(Tags) ->
	encode_bucket(Tags, <<"$annalist_counts">>).

encode_record_bucket(Tags) ->
	encode_bucket(Tags, <<"$annalist_records">>).

encode_bucket([], TagsBinary) ->
	TagsBinary;

encode_bucket([Tag | Tags], TagsBinary) when is_binary(Tag) ->
	TagsBinaryNew = <<TagsBinary/binary, <<"/">>/binary, Tag/binary>>,
	encode_bucket(Tags, TagsBinaryNew).

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

unsparse_range(ListSparse = [{KSparse, VSparse}|RestSparse], [Key | Keys], Default) ->
	case KSparse =:= Key of
		true ->
			[{KSparse, VSparse} | unsparse_range(RestSparse, Keys, Default)];
		false ->
			[{Key, Default} 	| unsparse_range(ListSparse, Keys, Default)]
	end;

unsparse_range([], Keys, Default) ->
	[{K, Default}|| K <- Keys];

unsparse_range(_, [], _) ->
	[].


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
	[?assertEqual(Time, annalist_utils:decode_time(annalist_utils:encode_time(Time))) || Time <- Times].

tag_encoding_test() ->
	?assertEqual(<<"$annalist_counts/first/second/third">>, annalist_utils:encode_count_bucket([<<"first">>, <<"second">>, <<"third">>])).

unsparse_range_test() ->
	Sparse = [{1, <<"one">>}, {2, <<"two">>}, {4, <<"four">>}],
	?assertEqual([{1, <<"one">>}, {2, <<"two">>}], annalist_utils:unsparse_range([{1, <<"one">>}, {2, <<"two">>}], [1, 2], undefined)),
	?assertEqual([{1, <<"one">>}, {2, <<"two">>}], annalist_utils:unsparse_range([{1, <<"one">>}, {2, <<"two">>}, {2, <<"three">>}], [1, 2], undefined)),
	?assertEqual([{1, <<"one">>}, {2, <<"two">>}, {3, undefined}, {4, <<"four">>}], annalist_utils:unsparse_range(Sparse, [1, 2, 3, 4], undefined)),
	?assertEqual([{1, <<"one">>}, {2, <<"two">>}, {3, undefined}, {4, <<"four">>}, {5, undefined}], annalist_utils:unsparse_range(Sparse, [1, 2, 3, 4, 5], undefined)),
	?assertEqual([{0, undefined}, {1, <<"one">>}, {2, <<"two">>}, {3, undefined}, {4, <<"four">>}], annalist_utils:unsparse_range(Sparse, [0, 1, 2, 3, 4], undefined)),
	?assertEqual([{-1, undefined}, {0, undefined}, {1, <<"one">>}, {2, <<"two">>}, {3, undefined}, {4, <<"four">>}, {5, undefined}, {6, undefined}], annalist_utils:unsparse_range(Sparse, [-1, 0, 1, 2, 3, 4, 5, 6], undefined)),
	?assertEqual([{0, undefined}, {1, undefined}, {2, undefined}, {3, undefined}], annalist_utils:unsparse_range([], [0, 1, 2, 3], undefined)).

-endif.
