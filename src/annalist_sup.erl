-module(annalist_sup).

-behaviour(supervisor).

%% API
-export([start_link/1]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

-spec start_link(list()) -> {ok, pid()} | {error, string()}.
start_link(ElevelDBDir) ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, [ElevelDBDir]).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

-spec init([list()]) -> {ok, {tuple(), [tuple()]}}.
init([ElevelDBDir]) ->
	AnnalistServer = {annalist, {annalist, start_link, [ElevelDBDir]},
		permanent, 1000, worker, [annalist, uplevel, eleveldb]},
    {ok, { {one_for_one, 5, 10}, [AnnalistServer]} }.