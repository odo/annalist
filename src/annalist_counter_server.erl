-module(annalist_counter_server).

-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

-export([
	queue_length/0
	, beacon/0
	, count/1, count/2
	, count_many/2, count_many/3
	, count_sync/1, count_sync/2
	, count_sparse/2, count_sparse/3
]).

-type tags() :: [binary()].
-type time() :: {{integer(), integer(), integer()}, {integer(), integer(), integer()}}.

% callbacks
-export ([init/1, stop/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/1]).

% API

queue_length() ->
	{_, Length}  = erlang:process_info(whereis(?SERVER), message_queue_len),
	Length.

-spec start_link(list()) -> 'ignore' | {'error',_} | {'ok',pid()}.
start_link(ElevelDBHandle) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [ElevelDBHandle], []).
	
stop() ->
	gen_server:call(?SERVER, {stop}).

-spec beacon() -> ok.
beacon() ->
	gen_server:call(?SERVER, {beacon}).

% count
-spec count_sync(tags()) -> ok.
count_sync(Tags) ->
	gen_server:call(?SERVER, {count, Tags, calendar:universal_time()}).

-spec count_sync(tags(), time()) -> ok.
count_sync(Tags, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:call(?SERVER, {count, Tags, Time}).

-spec count(tags()) -> ok.
count(Tags) ->
	gen_server:cast(?SERVER, {count, Tags, calendar:universal_time()}).

-spec count(tags(), time()) -> ok.
count(Tags, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:cast(?SERVER, {count, Tags, Time}).

-spec count_many(tags(), non_neg_integer()) -> ok.
count_many(Tags, Count) ->
	gen_server:cast(?SERVER, {count_many, Tags, Count, calendar:universal_time()}).

-spec count_many(tags(), non_neg_integer(), time()) -> ok.
count_many(Tags, Count, Time = {{_, _, _}, {_, _, _}}) ->
	gen_server:cast(?SERVER, {count_many, Tags, Count, Time}).

-spec count_sparse(tags(), non_neg_integer()) -> ok.
count_sparse(Tags, SparsenessFactor) ->
	count_sparse(Tags, calendar:universal_time(), SparsenessFactor).

-spec count_sparse(tags(), time(), non_neg_integer()) -> ok.
count_sparse(Tags, Time = {{_, _, _}, {_, _, _}}, SparsenessFactor) ->
	case random:uniform() < 1 / SparsenessFactor of
		true ->
			gen_server:cast(?SERVER, {count_inc, SparsenessFactor, Tags, Time});
		false ->
			nothing
	end.

% gen_server callbacks

init([ElevelDBHandle]) ->
	{ok, #state{handle = ElevelDBHandle}}.

handle_call({beacon}, _From, State) ->
	{reply, ok, State};

handle_call({count, Tags, Time}, _From, State) ->
	Res = counter:count(1, Tags, Time, State#state.handle),
	{reply, Res, State};

handle_call({count_inc, Increment, Tags, Time}, _From, State) ->
	Res = counter:count(Increment, Tags, Time, State#state.handle),
	{reply, Res, State};

handle_call({counts_with_labels, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts_with_labels(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({counts, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({leveldb_handle}, _From, State) ->
	{reply, State#state.handle, State};

handle_call({stop}, _From, State) ->
  {stop, normal, stopped, State}.

handle_cast({count, Tags, Time}, State) ->
	counter:count(1, Tags, Time, State#state.handle),
	{noreply, State};
	
handle_cast({count_many, Tags, Count, Time}, State) ->
	counter:count(Count, Tags, Time, State#state.handle),
	{noreply, State};

handle_cast({count_inc, Increment, Tags, Time}, State) ->
	counter:count(Increment, Tags, Time, State#state.handle),
	{noreply, State}.
	
handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
