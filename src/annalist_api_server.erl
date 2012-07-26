-module(annalist_api_server).

-define (SERVER, ?MODULE).

-behaviour (gen_server).
-record (state, {handle}).

-export([
	counts_with_labels/2, counts_with_labels/4,
	counts/2, counts/4,
	quantiles_with_labels/3, quantiles_with_labels/5,
	quantiles/3, quantiles/5,
	leveldb_handle/0
]).

-type tags() :: [binary()].
-type scope() :: year | month | day | hour | minute | second | total.
-type time() :: {integer(), integer(), integer(), integer(), integer(), integer()} |
				{integer(), integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer(), integer()} | 
				{integer(), integer(), integer()} | 
				{integer(), integer()} | 
				{integer()}. 

% callbacks
-export ([init/1, stop/0, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3, start_link/1]).


-spec start_link(list()) -> [{ok, pid()}].
start_link(ElevelDBHandle) ->
	gen_server:start_link({local, ?SERVER}, ?MODULE, [ElevelDBHandle], []).
	
stop() ->
	gen_server:call(?SERVER, {stop}).

-spec counts_with_labels(tags(), total) -> ok.
counts_with_labels(Tags, total) ->
	counts_with_labels(Tags, total, {}, 1).

-spec counts_with_labels(tags(), scope(), time(), integer()) -> ok.
counts_with_labels(Tags, Scope, TimeStart, Steps) ->
	validate(Scope, TimeStart),
	gen_server:call(?SERVER, {counts_with_labels, Tags, Scope, TimeStart, Steps}).

-spec quantiles_with_labels(tags(), total, number()) -> ok.
quantiles_with_labels(Tags, total, Quantile) ->
	quantiles_with_labels(Tags, total, {}, 1, Quantile).

-spec quantiles_with_labels(tags(), scope(), time(), integer(), number()) -> ok.
quantiles_with_labels(Tags, Scope, TimeStart, Steps, Quantile) ->
	validate(Scope, TimeStart),
	gen_server:call(?SERVER, {quantiles_with_labels, Tags, Scope, TimeStart, Steps, Quantile}).

-spec counts(tags(), total) -> ok.
counts(Tags, total) ->
	counts(Tags, total, {}, 1).

-spec counts(tags(), scope(), time(), integer()) -> ok.
counts(Tags, Scope, TimeStart, Steps) ->
	validate(Scope, TimeStart),
	gen_server:call(?SERVER, {counts, Tags, Scope, TimeStart, Steps}).

-spec quantiles(tags(), total, number()) -> ok.
quantiles(Tags, total, Quantile) ->
	quantiles(Tags, total, {}, 1, Quantile).

-spec quantiles(tags(), scope(), time(), integer(), number()) -> ok.
quantiles(Tags, Scope, TimeStart, Steps, Quantile) ->
	validate(Scope, TimeStart),
	gen_server:call(?SERVER, {quantiles, Tags, Scope, TimeStart, Steps, Quantile}).

leveldb_handle() ->
	gen_server:call(?SERVER, {leveldb_handle}).

% gen_server callbacks

init([ElevelDBHandle]) ->
	{ok, #state{handle = ElevelDBHandle}}.

handle_call({counts_with_labels, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts_with_labels(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({quantiles_with_labels, Tags, Scope, TimeStart, Steps, Quantile}, _From, State) ->
	Res = recorder:quantiles_with_labels(Tags, Scope, TimeStart, Steps, Quantile, State#state.handle),
	{reply, Res, State};

handle_call({counts, Tags, Scope, TimeStart, Steps}, _From, State) ->
	Res = counter:counts(Tags, Scope, TimeStart, Steps, State#state.handle),
	{reply, Res, State};

handle_call({quantiles, Tags, Scope, TimeStart, Steps, Quantile}, _From, State) ->
	Res = recorder:quantiles(Tags, Scope, TimeStart, Steps, Quantile, State#state.handle),
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

validate(total, 	{}) 				-> true;
validate(year, 		{_}) 				-> true;
validate(month, 	{_, _}) 			-> true;
validate(day, 		{_, _, _}) 			-> true;
validate(hour, 		{_, _, _, _}) 		-> true;
validate(minute, 	{_, _, _, _, _}) 	-> true;
validate(second, 	{_, _, _, _, _, _})	-> true;
validate(Scope, TimeStart)				-> throw({invlid_combination, [{scope, Scope}, {time_start, TimeStart}]}).
