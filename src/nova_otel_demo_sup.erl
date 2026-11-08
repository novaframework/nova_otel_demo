-module(nova_otel_demo_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Children = [otel_nova_prom_server:child_spec(#{port => 9464, ip => {0, 0, 0, 0}})],
    {ok, {#{strategy => one_for_all}, Children}}.
