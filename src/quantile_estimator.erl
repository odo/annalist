-module(quantile_estimator).

% Based on:
% Cormode et. al.:
% "Effective Computation of Biased Quantiles over Data Streams"

-export([
	new/0,
	insert/3,
	quantile/3,
	compress/2
]).

-export([
	f_biased/1,
	f_targeted/1
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DEBUG, true).
-endif.

-include("quantile_estimator.hrl").

-type data_sample() :: number().
-type data_structure() :: {number(), [#group{}]}.
-type invariant() :: fun((number(), number()) -> number()).

% group composition:
% 		t i 	 				t i+1 			t i+2
% |min i 		max i||min i+1	  max i+1||min i+2 	  max i+2|
% |__________________||__________________||__________________|
% |-----delta i--------|
%                   |-----delta i+1--------|
%                                       |-----delta i+2--------|
% |----------g i+1--|
% 					|----------g i+2----|

	
-spec f_biased(number()) -> invariant().
f_biased(Epsilon) ->
	fun(Rank, _N) ->
		2 * Epsilon * Rank
	end.

-spec f_targeted([{number(), number()}]) -> invariant().
f_targeted(Targets) ->
	TargetFuns = [f_targeted(Phi, Epsilon)||{Phi, Epsilon} <- Targets],
	fun(Rank, N) ->
		lists:min([TargetFun(Rank, N)||TargetFun<-TargetFuns])
	end.

-spec f_targeted(number(), number()) -> invariant().
f_targeted(Phi, Epsilon) ->
	fun(Rank, N) ->
		case Phi * N =< Rank of
			true ->
				(2 * Epsilon * Rank) / Phi;
			false ->
				(2 * Epsilon * (N - Rank)) / (1 - Phi)
		end
	end.

-spec new() -> data_structure().
new() ->
	{0, []}.

-spec insert(data_sample(), invariant(), data_structure()) -> data_structure().
insert(V, Invariant, {N, Data}) ->
	{N + 1, insert(V, Data, N, Invariant, 0)}.

% we terminate and the group has been added
insert(undefined, [], _N, _Invariant, _Rank) ->
	[];

% we terminate but the group has not been added yet
% so this is the maximum value
insert(V, [], _, _, _) ->
	[#group{v = V, g = 1, delta = 0}];

% the group has already been inserted, just append
insert(undefined, [Next|DataTail], N, _, undefined) ->
	[Next|insert(undefined, DataTail, N, undefiend, undefined)];

% the group has not yet been insterted
insert(V, [Next = #group{v = Vi, g = Gi}|DataTail], N, Invariant, RankLast) ->
	Ranki = RankLast + Gi,
	% did we pass a smaller Vi?
	case Vi >= V of
		true ->
			GroupNew = 
			case Ranki =:= 1 of
				true  -> #group{v = V, g = 1, delta = 0};
				false -> #group{v = V, g = 1, delta = clamp(floor(Invariant(Ranki, N)) - 1)}
			end,
			[GroupNew|[Next|insert(undefined, DataTail, N, undefiend, undefined)]];
		false ->
			[Next|insert(V, DataTail, N, Invariant, Ranki)]
	end.

-spec compress(invariant(), data_structure()) -> data_structure().
compress(Invariant, {N, Data}) ->
	{N, compress(Invariant, N, Data, 0, undefined)}.

compress(Invariant, N, [Next = #group{g = Gi} | Rest], RankLast, undefined)->
	Ranki = RankLast + Gi,
	compress(Invariant, N, Rest, Ranki, Next);

compress(_, _, [], _, Last) ->
	[Last];

compress(_, _, [Next], _, Last) ->
	[Last|[Next]];

compress(Invariant, N, [Next = #group{g = Giplusone, delta = Deltaiplusone} | Rest], Ranki, Last = #group{g = Gi}) ->
	% 		error_logger:info_msg("Rank:~p\n", [Ranki]),
	% error_logger:info_msg("comress ~p =< ~p \n", [(Gi + Giplusone + Deltaiplusone), Invariant(Ranki, N)]),
	% 		error_logger:info_msg("Last:~p\n", [Last]),
	case Gi + Giplusone + Deltaiplusone =< Invariant(Ranki, N) of
		true ->
			% [Last|[Next|compress(Invariant, N, Rest, Ranki + Gi, undefined)]];
			[merge(Last, Next)|compress(Invariant, N, Rest, Ranki + Gi, undefined)];
		false ->
			[Last|compress(Invariant, N, Rest, Ranki + Gi, Next)]
	end.

merge(#group{g = Gi}, #group{g = Giplusone, v = Viplusone, delta = Deltaiplusone}) ->
	% error_logger:info_msg("merging...GI:~p , Giplusone:~p\n", [Gi, Giplusone]),
	#group{v = Viplusone, g = Gi + Giplusone, delta = Deltaiplusone}.

-spec quantile(number(), invariant(), data_structure()) -> number().
quantile(_, _, {_, []}) ->
	throw({error, empty_stats});

quantile(Phi, Invariant, {N, [First|DataStructure]}) ->
	quantile(Phi, Invariant, DataStructure, N, 0, First).

quantile(_, _, [], _, _, #group{v = Vlast}) ->
	Vlast;

quantile(Phi, Invariant, [Next = #group{g = Gi, delta = Deltai}|DataStructure], N, Rank, #group{v = Vlast}) ->
	case (Rank + Gi + Deltai) > (Phi * N + Invariant(Phi * N, N) / 2) of
		true ->
			Vlast;
		false ->
			quantile(Phi, Invariant, DataStructure, N, Rank + Gi, Next)
	end.

floor(X) ->
    T = erlang:trunc(X),
    case (X - T) of
        Neg when Neg < 0 -> T - 1;
        Pos when Pos > 0 -> T;
        _ -> T
    end.

clamp(X) when X >= 0 -> X;
clamp(X) when X < 0 -> 0.


-ifdef(TEST).

quantile_estimator_test_() ->
[{foreach, local,
	fun test_setup/0,
	fun test_teardown/1,
	[
		% {"simple insert", fun test_insert/0}
		% ,{"quantiles are working", fun test_quantile/0}
		% ,{"quantiles are working with compression and biased quantiles", fun test_compression_biased/0}
		% ,{"quantiles are working with compression and targeted quantiles", fun test_comression_targeted/0}
		{"quantiles are working with long tail data set", timeout, 150, fun test_long_tail/0}
	]}
].

test_setup() -> nothing.
test_teardown(_) -> nothing.

test_insert() ->
	Invariant = f_biased(0.001),
	Insert = fun(Value, Data) -> insert(Value, Invariant, Data) end,
	QE1 = Insert(13, {0, []}),
	?assertEqual(
		{1, [#group{v = 13, g = 1, delta = 0}]},
	QE1),
	QE2 = Insert(2, QE1),
	?assertEqual(
		{2,
			[
			#group{v = 2,  g = 1, delta = 0},
			#group{v = 13, g = 1, delta = 0}
			]
		},
	QE2),
	QE3 = Insert(8, QE2),
	?assertEqual(
		{3,
			[
			#group{v = 2,  g = 1, delta = 0},
			#group{v = 8,  g = 1, delta = 0},
			#group{v = 13, g = 1, delta = 0}
			]
		},
	QE3),
	QE4 = Insert(-3, QE3),
	?assertEqual(
		{4,
			[
			#group{v = -3, g = 1, delta = 0},
			#group{v = 2,  g = 1, delta = 0},
			#group{v = 8,  g = 1, delta = 0},
			#group{v = 13, g = 1, delta = 0}
			]
		},
	QE4),
	QE5 = Insert(99, QE4),
	?assertEqual(
		{5,
			[
			#group{v = -3, g = 1, delta = 0},
			#group{v = 2,  g = 1, delta = 0},
			#group{v = 8,  g = 1, delta = 0},
			#group{v = 13, g = 1, delta = 0},
			#group{v = 99, g = 1, delta = 0}
			]
		},
	QE5),
	QE6 = Insert(14, QE5),
	?assertEqual(
		{6,
			[
			#group{v = -3, g = 1, delta = 0},
			#group{v = 2,  g = 1, delta = 0},
			#group{v = 8,  g = 1, delta = 0},
			#group{v = 13, g = 1, delta = 0},
			#group{v = 14, g = 1, delta = 0},
			#group{v = 99, g = 1, delta = 0}
			]
		},
	QE6).

test_quantile() ->
	% we create a set of 1000 random values and test if the guarantees are met
	Invariant = f_biased(0.001),
	N = 1000,
	Samples = [random:uniform()||_ <- lists:seq(1, N)],
	Data = lists:foldl(fun(Sample, Stats) -> insert(Sample, Invariant, Stats) end, {0, []}, Samples),
	% error_logger:info_msg("D:~p\n", [D]),
	validate(Samples, Invariant, Data),
	Samples2 = [5],
	Data2 = lists:foldl(fun(Sample, Stats) -> insert(Sample, Invariant, Stats) end, {0, []}, Samples2),
	?assertEqual(5, quantile(0, Invariant, Data2)),
	?assertEqual(5, quantile(1, Invariant, Data2)),
	?assertEqual(5, quantile(0.99, Invariant, Data2)).

	
test_compression_biased() ->
	% we create a set of 1000 random values and test if the guarantees are met
	Invariant = f_biased(0.01),
	N = 2000,
	Samples = [random:uniform()||_ <- lists:seq(1, N)],
	Data = lists:foldl(fun(Sample, Stats) -> insert(Sample, Invariant, Stats) end, {0, []}, Samples),
	{_, DL} = Data,
	validate(Samples, Invariant, Data),
	compress_and_validate(Samples, Invariant, Data, length(DL)).

test_comression_targeted() ->
	% we create a set of 1000 random values and test if the guarantees are met
	Invariant = quantile_estimator:f_targeted([{0.05, 0.005}, {0.5, 0.02}, {0.95, 0.005}]),
	N = 2000,
	Samples = [random:uniform()||_ <- lists:seq(1, N)],
	Data = lists:foldl(fun(Sample, Stats) -> insert(Sample, Invariant, Stats) end, {0, []}, Samples),
	{_, DL} = Data,
	validate(Samples, Invariant, Data),
	compress_and_validate(Samples, Invariant, Data, length(DL)).

test_long_tail() ->
	{ok, [Samples]} = file:consult(os:getenv("TESTDIR") ++ "us_city_populations"),
	Invariant = quantile_estimator:f_targeted([{0.01, 0.005}, {0.05, 0.005}, {0.5, 0.025}, {0.95, 0.005}, {0.99, 0.005}]),
	% Invariant = quantile_estimator:f_biased(0.001),
	lists:foldl(
		fun(Sample, {Stats = {_, DL}, SamplesUsed}) ->
			StatsNew = insert(Sample, Invariant, Stats),
			SamplesNew = [Sample|SamplesUsed],
			io:format("StatsNew:~p\n", [StatsNew]),
			validate(SamplesNew, Invariant, StatsNew),
			StatsCompressed =
			case length(SamplesUsed) rem 10 =:= 0 of
				true ->
					compress_and_validate(SamplesNew, Invariant, StatsNew, length(DL));
					% validate(SamplesNew, Invariant, StatsNew),
					% StatsNew;
				false ->
					StatsNew
			end,
			% StatsNewCompress = compress(Invariant, StatsNew),
			% validate(Samples, Invariant, StatsNewCompress),
			% StatsNewCompress
			{StatsCompressed, SamplesNew}
		end,
		{{0, []}, []},
		Samples
	).

validate(Samples, Invariant, Data = {N, _}) ->
	Index = fun(Element, List) -> length(lists:takewhile(fun(E) -> E < Element end, List)) end,
	SamplesSort = lists:sort(Samples),
	Quantiles = [0.01, 0.05, 0.10, 0.5, 0.90, 0.95, 0.99],
	QantileEstimateDevAlloweddev = [
		{
			Index(quantile:quantile(Q, SamplesSort), SamplesSort), 
			Index(quantile(Q, Invariant, Data), SamplesSort), 
			abs(Index(quantile:quantile(Q, SamplesSort), SamplesSort) - Index(quantile(Q, Invariant, Data), SamplesSort)),
			quantile:ceil(Invariant(Index(quantile(Q, Invariant, Data), SamplesSort), N))
		} || Q <- Quantiles],
	error_logger:info_msg("N:~p, QantileEstimateDevAlloweddev:~p\n", [N, QantileEstimateDevAlloweddev]),
	[?assertEqual(true, (Dev =< DevAlloweddev)) || {_, _, Dev, DevAlloweddev} <- QantileEstimateDevAlloweddev].
			
% validate(Samples, Invariant, Data = {N, _}) ->
% 	SamplesSort = lists:sort(Samples),
% 	RankEstimate = [{R, quantile((R-1)/N, Invariant, Data)}||R<-[1, N*0.01, N*0.05, N*0.10, N*0.5, N*0.90, N*0.95, N*0.99, N]],
% 	RankEstimateRank = [{Rank, string:str(SamplesSort, [Estimate])}||{Rank, Estimate} <- RankEstimate],
% 	error_logger:info_msg("N:~p\n", [N]),
% 	error_logger:info_msg("RankEstimateRank:~p\n", [RankEstimateRank]),
% 	DeviationAllowedDeviation = [{abs(Rank - EstimateRank), Invariant(Rank, N)} || {Rank, EstimateRank} <- RankEstimateRank],
% 	error_logger:info_msg("DeviationAllowedDeviation:~p\n", [DeviationAllowedDeviation]),
% 	[?assert(Deviation =< AllowedDeviation)||{Deviation, AllowedDeviation} <- DeviationAllowedDeviation].

compress_and_validate(Samples, Invariant, Data = {N, _}, SizeLast) ->
	DataCompressed = compress(Invariant, Data),
	{{N, List}, {N, ListCompressed}} = {Data, DataCompressed},
	error_logger:info_msg("------->reduced from :~p to: ~p\n", [length(List), length(ListCompressed)]),
	?assert(length(ListCompressed) =< length(List)),
	% error_logger:info_msg("DataCompressed:~p\n", [DataCompressed]),
	validate(Samples, Invariant, DataCompressed),
	case length(ListCompressed) < SizeLast of
		true ->
			compress_and_validate(Samples, Invariant, DataCompressed, length(ListCompressed));
		false ->
			DataCompressed
	end.

-endif.

