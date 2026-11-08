-module(nova_otel_demo_router).
-behaviour(nova_router).
-export([routes/1]).

routes(_Environment) ->
    [
        #{
            prefix => "",
            security => false,
            routes => [
                {"/hello", fun demo_controller:hello/1, #{methods => [get]}},
                {"/echo", fun demo_controller:echo/1, #{methods => [post]}},
                {"/slow", fun demo_controller:slow/1, #{methods => [get]}}
            ]
        }
    ].
