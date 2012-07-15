-module(annalist_recorder_server).

-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

-export([
	queue_length/0
	, beacon/0
	, record/2, record/3
	, record_sync/2, record_sync/3
	, record_sparse/3, record_sparse/4
]).

-type tags() :: [binary()].
-type time() :: {integer(), integer(), integer(), integer(), integer(), integer()}.

% callbacks
-export ([init/1, stop/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/1]).

% API

queue_length() ->
	{_, Length}  = erlang:process_info(whereis(?SERVER), message_queue_len),
	Length.

-spec start_link(list()) -> [{ok, pid()}].
start_link(ElevelDBHandle) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [ElevelDBHandle], []).
	
stop() ->
	gen_server:call(?SERVER, {stop}).

-spec beacon() -> ok.
beacon() ->
	gen_server:call(?SERVER, {beacon}).

% record
-spec record_sync(tags(), number()) -> ok.
record_sync(Tags, Value) ->
	gen_server:call(?SERVER, {record, Tags, Value, calendar:universal_time()}).

-spec record_sync(tags(), number(), time()) -> ok.
record_sync(Tags, Value, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:call(?SERVER, {record, Tags, Value, Time}).

-spec record(tags(), number()) -> ok.
record(Tags, Value) ->
	gen_server:cast(?SERVER, {record, Tags, Value, calendar:universal_time()}).

-spec record(tags(), number(), time()) -> ok.
record(Tags, Value, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:cast(?SERVER, {record, Tags, Value, Time}).

-spec record_sparse(tags(), number(), non_neg_integer()) -> ok.
record_sparse(Tags, Value, SparsenessFactor) ->
	record_sparse(Tags, Value, calendar:universal_time(), SparsenessFactor).

-spec record_sparse(tags(), number(), time(), non_neg_integer()) -> ok.
record_sparse(Tags, Value, Time = {{_, _, _}, {_, _, _}}, SparsenessFactor) ->
	case random:uniform() < 1 / SparsenessFactor of
		true ->
			gen_server:cast(?SERVER, {record, Tags, Value, Time}); 
		false ->
			nothing
	end.

% gen_server callbacks

init([ElevelDBHandle]) ->
	{ok, #state{handle = ElevelDBHandle}}.

handle_call({beacon}, _From, State) ->
	{reply, ok, State};

handle_call({record, Tags, Value, Time}, _From, State) ->
	Res = recorder:record(Value, Tags, Value, Time, State#state.handle),
	{reply, Res, State};

handle_call({stop}, _From, State) ->
  {stop, normal, stopped, State}.

handle_cast({record, Tags, Value, Time}, State) ->
	recorder:record(Value, Tags, Time, State#state.handle),
	{noreply, State}.

handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
