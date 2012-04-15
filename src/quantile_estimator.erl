-module(quantile_estimator).

% Based on:
% Cormode et. al.:
% "Effective Computation of Biased Quantiles over Data Streams"

-export([
	insert/2,
	quantile/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(DEBUG, true).
-endif.

-include("quantile_estimator.hrl").

-type data_sample() :: number().
-type data_structure() :: [#group{}].

-define(EPSILON, 1).

% group composition:
% 		t i 	 				t i+1 			t i+2
% |min i 		max i||min i+1	  max i+1||min i+2 	  max i+2|
% |__________________||__________________||__________________|
% |-----delta i------||-----delta i+1----||-----delta i+2----|
% |----------g i+1----|
% 					  |----------g i+2----|

	

invariant(Rank, _N) ->
	?EPSILON * Rank.

-spec insert(data_sample(), data_structure()) -> data_structure().
insert(V, DataStructure) ->
	insert(V, DataStructure, length(DataStructure), 0).

% we terminate and the group has been added
insert(undefined, [], _N, _Rank) ->
	[];

% we terminate but the group has not been added yet
% so this is the maximum value
insert(V, [], _, _) ->
	[#group{v = V, g = 1, delta = 0}];

% the group has already been inserted, just append
insert(undefined, [Next|DataStructureTail], N, undefined) ->
	[Next|insert(undefined, DataStructureTail, N, undefined)];

% the group has not yet been insterted 
insert(V, [Next = #group{v = Vi, g = Gi}|DataStructureTail], N, RankLast) ->
	Ranki = RankLast + Gi,
	% did we pass a smaller Vi?
	case V < Vi of
		true ->
			% maybe it's the min value
			GroupNew = 
			case RankLast =:= 0 of
				true  -> #group{v = V, g = 1, delta = 0};
				false -> #group{v = V, g = 1, delta = invariant(Ranki, N) - 1}
			end,
			[GroupNew|[Next|insert(undefined, DataStructureTail, N, undefined)]];
		false ->
			[Next|insert(V, DataStructureTail, N, Ranki)]
	end.

-spec quantile(number(), data_structure()) -> number().
quantile(_, []) ->
	throw({error, empty_stats});

quantile(Phi, [First|DataStructure]) ->
	quantile(Phi, DataStructure, length(DataStructure), 0, First).

quantile(_, [], _, _, #group{v = Vlast}) ->
	Vlast;

quantile(Phi, [Next = #group{g = Gi, delta = Deltai}|DataStructure], N, Rank, #group{v = Vlast}) ->
	case (Rank + Gi + Deltai) > (Phi * N + invariant(Phi * N, N) / 2) of
		true ->
			Vlast;
		false ->
			quantile(Phi, DataStructure, N, Rank + Gi, Next)
	end.

-ifdef(TEST).

insert_test() ->
	QE1 = insert(13, []),
	?assertEqual(
		[#group{v = 13, g = 1, delta = invariant(0, 0) - 1}],
	QE1),
	QE2 = insert(2, QE1),
	?assertEqual(
		[
		#group{v = 2,  g = 1, delta = invariant(1, 1) - 1},
		#group{v = 13, g = 1, delta = invariant(0, 0) - 1}
		],
	QE2),
	QE3 = insert(8, QE2),
		?assertEqual(
		[
		#group{v = 2,  g = 1, delta = invariant(1, 1) - 1},
		#group{v = 8,  g = 1, delta = invariant(2, 2) - 1},
		#group{v = 13, g = 1, delta = invariant(0, 0) - 1}
		],
	QE3),
	QE4 = insert(-3, QE3),
		?assertEqual(
		[
		#group{v = -3, g = 1, delta = invariant(1, 3) - 1},
		#group{v = 2,  g = 1, delta = invariant(1, 1) - 1},
		#group{v = 8,  g = 1, delta = invariant(2, 2) - 1},
		#group{v = 13, g = 1, delta = invariant(0, 0) - 1}
		],
	QE4),
	QE5 = insert(99, QE4),
		?assertEqual(
		[
		#group{v = -3, g = 1, delta = invariant(1, 3) - 1},
		#group{v = 2,  g = 1, delta = invariant(1, 1) - 1},
		#group{v = 8,  g = 1, delta = invariant(2, 2) - 1},
		#group{v = 13, g = 1, delta = invariant(0, 0) - 1},
		#group{v = 99, g = 1, delta = invariant(4, 4) - 1}
		],
	QE5),
	QE6 = insert(14, QE5),
		?assertEqual(
		[
		#group{v = -3, g = 1, delta = invariant(1, 3) - 1},
		#group{v = 2,  g = 1, delta = invariant(1, 1) - 1},
		#group{v = 8,  g = 1, delta = invariant(2, 2) - 1},
		#group{v = 13, g = 1, delta = invariant(0, 0) - 1},
		#group{v = 14, g = 1, delta = invariant(5, 5) - 1},
		#group{v = 99, g = 1, delta = invariant(4, 4) - 1}
		],
	QE6).

quantile_test() ->
	Q1 = quantile(0.5, insert(13, [])),
	?assertEqual(13, Q1),
	Q2 = quantile(0.5, insert(1, Q1)).

-endif.

