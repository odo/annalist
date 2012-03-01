-module(date_ranges).
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-define(TESTDB, "/tmp/eleveldb.open.test").
-endif.

-export([
	year_range/2,
	month_range/2,
	day_range/2,
	hour_range/2,
	minute_range/2,
	second_range/2
]).

year_range({YearStart}, YearCount)
	when YearCount > 0 ->
	[{Y} || Y <- lists:seq(YearStart, (YearStart + YearCount - 1))].

month_range({YearStart, MonthStart}, MonthCount)
	when MonthStart > 0, MonthStart < 13, MonthCount > 0 ->
	lists:reverse(month_range({YearStart, MonthStart}, MonthCount, 0, [])).		

month_range({Year, Month}, MonthCount, MonthCurrent, Results) ->
	case MonthCurrent =:= MonthCount of
		true -> Results;
		false ->
			case Month =:= 12 of
				true ->
					month_range({Year + 1, 1}, MonthCount, MonthCurrent + 1, [{Year, Month} | Results]);
				false ->
					month_range({Year, Month + 1}, MonthCount, MonthCurrent + 1, [{Year, Month} | Results])
			end
	end.

day_range({YearStart, MonthStart, DayStart}, DayCount)
	when MonthStart > 0, MonthStart < 13, DayStart > 0, DayStart < 32, DayCount > 0 ->
	GregorianDayStart = calendar:date_to_gregorian_days({YearStart, MonthStart, DayStart}), 
	lists:reverse(day_range(calendar:gregorian_days_to_date(GregorianDayStart), DayCount, GregorianDayStart, 1, [])).

day_range(Date, DayCount, GregorianDayCurrent, DayCurrent, Results) ->
	case DayCurrent > DayCount of
		true ->
			Results;
		false ->
			day_range(calendar:gregorian_days_to_date(GregorianDayCurrent + 1), DayCount, GregorianDayCurrent + 1, DayCurrent + 1, [Date | Results])
	end.

hour_range({YearStart, MonthStart, DayStart, HourStart}, HourCount)
	when MonthStart > 0, MonthStart < 13, DayStart > 0, DayStart < 32, HourStart > -1, HourStart < 24, HourCount > 0 ->
	GregorianDayStart = calendar:date_to_gregorian_days({YearStart, MonthStart, DayStart}), 
	lists:reverse(hour_range({YearStart, MonthStart, DayStart, HourStart}, HourCount, 1, GregorianDayStart, [])).

hour_range({Year, Month, Day, Hour}, HourCount, HourCurrent, GregorianDayCurrent, Results) ->
	case HourCurrent > HourCount of
		true ->
			Results;
		false ->
			case Hour =:= 23 of
				true ->
					{YearNew, MonthNew, DayNew} = calendar:gregorian_days_to_date(GregorianDayCurrent + 1),
					hour_range({YearNew, MonthNew, DayNew, 0}, 	HourCount, HourCurrent + 1, GregorianDayCurrent + 1, 	[{Year, Month, Day, Hour} | Results]);
				false ->
					hour_range({Year, Month, Day, Hour + 1}, 	HourCount, HourCurrent + 1, GregorianDayCurrent, 		[{Year, Month, Day, Hour} | Results])
			end
	end.

minute_range({YearStart, MonthStart, DayStart, HourStart, MinuteStart}, MinuteCount)
	when MonthStart > 0, MonthStart < 13, DayStart > 0, DayStart < 32, HourStart > -1, HourStart < 24, MinuteStart > -1, MinuteStart < 60, MinuteCount > 0 ->
	GregorianDayStart = calendar:date_to_gregorian_days({YearStart, MonthStart, DayStart}), 
	lists:reverse(minute_range({YearStart, MonthStart, DayStart, HourStart, MinuteStart}, MinuteCount, 1, HourStart, GregorianDayStart, [])).

minute_range({Year, Month, Day, Hour, Minute}, MinuteCount, MinuteCurrent, HourCurrent, GregorianDayCurrent, Results) ->
	case MinuteCurrent > MinuteCount of
		true ->
			Results;
		false ->
			case Minute =:= 59 of
				true ->
					case Hour =:= 23 of
						true ->
							{YearNew, MonthNew, DayNew} = calendar:gregorian_days_to_date(GregorianDayCurrent + 1),
							minute_range({YearNew, MonthNew, DayNew, 0, 0}, 	MinuteCount, MinuteCurrent + 1, 0, 					GregorianDayCurrent + 1, 	[{Year, Month, Day, Hour, Minute} | Results]);
						false ->
							minute_range({Year, Month, Day, Hour + 1,   0}, 	MinuteCount, MinuteCurrent + 1, HourCurrent + 1, 	GregorianDayCurrent, 		[{Year, Month, Day, Hour, Minute} | Results])
					end;
				false ->
							minute_range({Year, Month, Day, Hour, Minute + 1}, 	MinuteCount, MinuteCurrent + 1, HourCurrent, 		GregorianDayCurrent, 		[{Year, Month, Day, Hour, Minute} | Results])
			end
	end.

second_range({YearStart, MonthStart, DayStart, HourStart, MinuteStart, SecondStart}, SecondCount)
	when MonthStart > 0, MonthStart < 13, DayStart > 0, DayStart < 32, HourStart > -1, HourStart < 24, MinuteStart > -1, MinuteStart < 60, SecondStart > -1, SecondStart < 60, SecondCount > 0 ->
	GregorianSecondStart = calendar:datetime_to_gregorian_seconds({{YearStart, MonthStart, DayStart}, {HourStart, MinuteStart, SecondStart}}),
	lists:reverse(second_range({YearStart, MonthStart, DayStart, HourStart, MinuteStart, SecondStart}, SecondCount, 1, GregorianSecondStart, [])).

second_range({Year, Month, Day, Hour, Minute, Second}, SecondCount, SecondCurrent, GregorianSecondCurrent, Results) ->
	case SecondCurrent > SecondCount of
		true ->
			Results;
		false ->
			{{YearNew, MonthNew, DayNew}, {HourNew, MinuteNew, SecondNew}} = calendar:gregorian_seconds_to_datetime(GregorianSecondCurrent + 1),
			second_range({YearNew, MonthNew, DayNew, HourNew, MinuteNew, SecondNew}, SecondCount, SecondCurrent + 1, GregorianSecondCurrent + 1, [{Year, Month, Day, Hour, Minute, Second} | Results])
	end.

%% ===================================================================
%% EUnit tests
%% ===================================================================

-ifdef(TEST).

store_test_() ->
    [{foreach, local,
		fun test_setup/0,
      	fun test_teardown/1,
      [
      	{"range of years", fun test_year_range/0},
      	{"range of months", fun test_month_range/0},
      	{"range of days", fun test_day_range/0},
      	{"range of hours", fun test_hour_range/0},
      	{"range of minutes", fun test_minute_range/0},
      	{"range of seconds", fun test_second_range/0}
		]}
	].

test_setup() ->
	nothing.
 
test_teardown(_) ->
	nothing.

test_year_range()  ->
    ?assertEqual([{1998}, {1999}, {2000}, {2001}], year_range({1998}, 4)),
    ?assertEqual([{2525}], year_range({2525}, 1)).

test_month_range()  ->
    ?assertEqual([{2001, 10}, {2001, 11}, {2001, 12}], month_range({2001, 10}, 3)),
    ?assertEqual([{2001, 11}, {2001, 12}, {2002, 1}], month_range({2001, 11}, 3)).

test_day_range()  ->
    ?assertEqual([{2001, 10, 1}, {2001, 10, 2}, {2001, 10, 3}], day_range({2001, 10, 1}, 3)),
    ?assertEqual([{2001, 12, 30}, {2001, 12, 31}, {2002, 1, 1}], day_range({2001, 12, 30}, 3)),
    ?assertEqual([{2012, 2, 28}, {2012, 2, 29}, {2012, 3, 1}], day_range({2012, 2, 28}, 3)).

test_hour_range()  ->
    ?assertEqual([{2001, 10, 1, 13}, {2001, 10, 1, 14}, {2001, 10, 1, 15}], hour_range({2001, 10, 1, 13}, 3)),
    ?assertEqual([{2001, 12, 1, 23}, {2001, 12, 2, 0}, {2001, 12, 2, 1}], hour_range({2001, 12, 1, 23}, 3)),
    ?assertEqual([{2001, 1, 31, 23}, {2001, 2, 1, 0}, {2001, 2, 1, 1}], hour_range({2001, 1, 31, 23}, 3)),
    ?assertEqual([{2001, 12, 31, 23}, {2002, 1, 1, 0}, {2002, 1, 1, 1}], hour_range({2001, 12, 31, 23}, 3)).

test_minute_range()  ->
    ?assertEqual([{2001, 10, 1, 13, 14}, {2001, 10, 1, 13, 15}, {2001, 10, 1, 13, 16}], minute_range({2001, 10, 1, 13, 14}, 3)),
    ?assertEqual([{2001, 10, 1, 13, 58}, {2001, 10, 1, 13, 59}, {2001, 10, 1, 14, 0}], minute_range({2001, 10, 1, 13, 58}, 3)),
    ?assertEqual([{2001, 10, 1, 23, 59}, {2001, 10, 2, 0, 0}, {2001, 10, 2, 0, 1}], minute_range({2001, 10, 1, 23, 59}, 3)),
    ?assertEqual([{2001, 1, 31, 23, 59}, {2001, 2, 1, 0, 0}, {2001, 2, 1, 0, 1}], minute_range({2001, 1, 31, 23, 59}, 3)),
    ?assertEqual([{2001, 12, 31, 23, 59}, {2002, 1, 1, 0, 0}, {2002, 1, 1, 0, 1}], minute_range({2001, 12, 31, 23, 59}, 3)).

test_second_range()  ->
    ?assertEqual([{2001, 10, 1, 13, 14, 11}, {2001, 10, 1, 13, 14, 12}, {2001, 10, 1, 13, 14, 13}], second_range({2001, 10, 1, 13, 14, 11}, 3)),
    ?assertEqual([{2001, 10, 1, 13, 14, 59}, {2001, 10, 1, 13, 15, 0}, {2001, 10, 1, 13, 15, 1}], second_range({2001, 10, 1, 13, 14, 59}, 3)),
    ?assertEqual([{2001, 10, 1, 13, 59, 59}, {2001, 10, 1, 14, 0, 0}, {2001, 10, 1, 14, 0, 1}], second_range({2001, 10, 1, 13, 59, 59}, 3)),
    ?assertEqual([{2001, 10, 1, 23, 59, 59}, {2001, 10, 2, 0, 0, 0}, {2001, 10, 2, 0, 0, 1}], second_range({2001, 10, 1, 23, 59, 59}, 3)),
    ?assertEqual([{2001, 1, 31, 23, 59, 59}, {2001, 2, 1, 0, 0, 0}, {2001, 2, 1, 0, 0, 1}], second_range({2001, 1, 31, 23, 59, 59}, 3)),
    ?assertEqual([{2001, 12, 31, 23, 59, 59}, {2002, 1, 1, 0, 0, 0}, {2002, 1, 1, 0, 0, 1}], second_range({2001, 12, 31, 23, 59, 59}, 3)).

-endif.