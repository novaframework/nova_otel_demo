-module(nova_otel_demo_app).
-behaviour(application).
-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    ok = opentelemetry_nova:setup(),
    nova_otel_demo_sup:start_link().

stop(_State) -> ok.
