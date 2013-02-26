%%  Copyright (C) 2011 - Molchanov Maxim,
%% @author Maxim Molchanov <elzor.job@gmail.com>

-module(cass_sup).

-behaviour(supervisor).

%% API
-export([start_link/0, pick_worker/0, call/2]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    {ok, Size} = config:get(cassandra_pool_size),
    Workers = [ worker_spec(I) || I <- lists:seq(1, Size) ],
    {ok, {{one_for_one, 10, 1}, Workers}}.

%% ===================================================================
%% Internal API
%% ===================================================================

worker_spec(N) ->
    Name = list_to_atom("ecass" ++ integer_to_list(N)),
    {Name,
        {cassanderl, start_link, [Name]},
        permanent, 1000, worker,
        [cassanderl]
    }.

pick_worker() ->
    random:seed(erlang:now()),
    {ok, Size} = config:get(cassandra_pool_size),
    list_to_existing_atom("ecass" ++ integer_to_list(random:uniform(Size))).

call(Function, Args) ->
    Worker = pick_worker(),
    gen_server:call(Worker, {call, Function, Args}).
