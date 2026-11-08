-module(nova_otel_demo_sup).
-behaviour(supervisor).
-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Metrics = #{
        port => application:get_env(nova_otel_demo, metrics_port, 9464),
        ip => application:get_env(nova_otel_demo, metrics_ip, {127, 0, 0, 1})
    },
    {ok, {#{strategy => one_for_all}, [otel_nova_prom_server:child_spec(Metrics)]}}.
