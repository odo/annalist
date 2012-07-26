-module(annalist_utils).

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