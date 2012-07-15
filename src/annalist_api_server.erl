-module(annalist_api_server).

-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

-export([
	queue_length/0,
	counts_with_labels/4,
	counts/4,
	leveldb_handle/0
]).

-type tags() :: [binary()].
-type scope() :: year | month | day | hour | minute | second.
-type time() :: {integer(), integer(), integer(), integer(), integer(), integer()} |
				{integer(), integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer()} | 
				{integer(), integer()} | 
				{integer()}. 

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


-spec counts_with_labels(tags(), scope(), time(), integer()) -> ok.
counts_with_labels(Tags, year, TimeStart = {_}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, year, TimeStart, Steps});

counts_with_labels(Tags, month, TimeStart = {_, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, month, TimeStart, Steps});

counts_with_labels(Tags, day, TimeStart = {_, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, day, TimeStart, Steps});

counts_with_labels(Tags, hour, TimeStart = {_, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, hour, TimeStart, Steps});

counts_with_labels(Tags, minute, TimeStart = {_, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, minute, TimeStart, Steps});

counts_with_labels(Tags, second, TimeStart = {_, _, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts_with_labels, Tags, second, TimeStart, Steps}).


-spec counts(tags(), scope(), time(), integer()) -> ok.
counts(Tags, year, TimeStart = {_}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, year, TimeStart, Steps});

counts(Tags, month, TimeStart = {_, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, month, TimeStart, Steps});

counts(Tags, day, TimeStart = {_, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, day, TimeStart, Steps});

counts(Tags, hour, TimeStart = {_, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, hour, TimeStart, Steps});

counts(Tags, minute, TimeStart = {_, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, minute, TimeStart, Steps});

counts(Tags, second, TimeStart = {_, _, _, _, _, _}, Steps) ->
	gen_server:call(?SERVER, {counts, Tags, second, TimeStart, Steps}).


leveldb_handle() ->
	gen_server:call(?SERVER, {leveldb_handle}).

% gen_server callbacks

init([ElevelDBHandle]) ->
	{ok, #state{handle = ElevelDBHandle}}.

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

handle_cast(_Msg, _State) ->
  throw(not_implemented),
  {undefined, undefined, undefined, undefined}.

handle_info(_Info, State) ->
	{noreply, State}.
	
terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.
