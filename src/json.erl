-module (json).
-export ([
	from/1, 
	to/1,
	to_binary/1, to_binary/2
]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

to_binary(Data) ->
	to_binary(Data, undefined).

to_binary(Data, Callback) ->
	JSON = to(Data),
	case Callback of
		undefined ->
			list_to_binary(JSON);
		_ ->
			list_to_binary(binary_to_list(Callback) ++ "(" ++ JSON ++ ");")
	end.

to(Data) ->
	rfc4627:encode(Data).

from(String) ->
	{ok, Data, _} = rfc4627:decode(String),
	parse_json_element(Data).

parse_json_element(E) when is_list(E) ->
	[parse_json_element(E2) || E2 <- E];
	
parse_json_element({obj, L}) when is_list(L) ->
	dict:from_list(parse_json_element(L));

parse_json_element({E1, E2} = T) when is_tuple(T) ->
	{E1, parse_json_element(E2)};	
	
parse_json_element(E) ->
	E.


%% ===================================================================
%% Unit Tests
%% ===================================================================
-ifdef(TEST).

% dicts
dict_string_test() ->
	J = "{\"string\":\"hoho\"}",
	D = dict:from_list([{"string", <<"hoho">>}]),
	test_back_and_forth(J, D).
	
dict_integer_test() ->
	J = "{\"integer\":12345}",
	D = dict:from_list([{"integer", 12345}]),
	test_back_and_forth(J, D).
	
dict_float_test() ->
	J = "{\"float\":1.2345}",
	D = dict:from_list([{"float", 1.2345}]),
	test_data(J, D).

dict_list_test() ->
	J = "{\"float\":[1,2,3,4]}",
	D = dict:from_list([{"float", [1,2,3,4]}]),
	test_back_and_forth(J, D).

% lists
list_string_test() ->
	J = "[\"string\",\"hoho\"]",
	D = [<<"string">>, <<"hoho">>],
	test_back_and_forth(J, D).
	
list_integer_test() ->
	J = "[1,2,3,4]",
	D = [1,2,3,4],
	test_back_and_forth(J, D).
	
list_float_test() ->
	J = "[1.1, 2.2, 3.3]",
	D = [1.1, 2.2, 3.3],
	test_data(J, D).

list_list_test() ->
	J = "[1,2,3,[1,2,3,4]]",
	D = [1,2,3,[1,2,3,4]],
	test_back_and_forth(J, D).	

% nested
nested_dict_test() ->
	J = "{\"list\":[1,2,{\"nested_key\":\"nested_val\"}]}",
	D = dict:from_list(
		[{"list", [
			1,2,dict:from_list([{"nested_key", <<"nested_val">>}])
			]}]),
	test_back_and_forth(J, D).	
	
nested_list_test() ->
	J = "[\"list\",{\"nested_key\":[1,2,3,\"nested_val\"]}]",
	D = [<<"list">>, dict:from_list([{"nested_key", [1,2,3,<<"nested_val">>]}])],
	test_back_and_forth(J, D).

% helper
test_back_and_forth(J, D) ->
	test_data(J, D),
	test_string(J, D).
	
test_data(J, D) ->
	?assertEqual(D, from(J)),
	?assertEqual(D, from(to(D))).

test_string(J, D) ->
	?assertEqual(J, to(D)),
	?assertEqual(J, to(from(J))).

% test_data() ->
% 	[<<"test">>, [1,2,3], dict:from_list([{"string", <<"hoho">>}, {"float", 1.2}, {"integer", 5}])].
% 	
% test_string() ->
% 	"[\"test\",[1,2,3],{\"string\":\"hoho\", \"float\": 1.2, \"integer\": 5}]".
% 
% redecode_test() ->
% 	?assertEqual(test_data(), from(to(test_data()))).
% 
% decoding_test() ->
% 	?assertEqual(test_data(), from(test_string())).

-endif.