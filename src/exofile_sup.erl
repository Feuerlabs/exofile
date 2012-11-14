
-module(exofile_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% Helper macro for declaring children of supervisor
-define(CHILD(M, Type), {M, {M, start_link, []}, permanent, 5000, Type, [M]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    C = ?CHILD(exofile_srv, worker),
    {ok, { {one_for_one, 5, 10}, [C]} }.

