-module(nova_otel_demo_SUITE).
-behaviour(ct_suite).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("opentelemetry/include/otel_span.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2]).
-export([
    request_is_traced_with_route_and_controller/1,
    plugins_after_otel_still_run/1,
    otel_plugin_runs_before_the_request_plugin/1,
    incoming_trace_context_is_continued/1,
    prometheus_endpoint_counts_the_request/1
]).

-define(HTTP_PORT, 18080).
-define(METRICS_PORT, 19464).
-define(DEADLINE_MS, 5000).

all() ->
    [
        request_is_traced_with_route_and_controller,
        plugins_after_otel_still_run,
        otel_plugin_runs_before_the_request_plugin,
        incoming_trace_context_is_continued,
        prometheus_endpoint_counts_the_request
    ].

init_per_suite(Config) ->
    [load(App) || App <- [nova, nova_otel_demo, opentelemetry]],
    {ok, #{} = Cowboy} = application:get_env(nova, cowboy_configuration),
    ok = application:set_env(nova, cowboy_configuration, Cowboy#{port => ?HTTP_PORT}),
    ok = application:set_env(nova_otel_demo, metrics_port, ?METRICS_PORT),
    ok = application:set_env(nova_otel_demo, metrics_ip, {127, 0, 0, 1}),
    ok = application:set_env(opentelemetry, traces_exporter, none),
    ok = application:set_env(opentelemetry, bsp_scheduled_delay_ms, 50),
    {ok, Started} = application:ensure_all_started(nova_otel_demo),
    [{started, Started} | Config].

end_per_suite(Config) ->
    [application:stop(App) || App <- lists:reverse(?config(started, Config)), is_atom(App)],
    ok.

init_per_testcase(_TestCase, Config) ->
    otel_batch_processor:set_exporter(otel_exporter_pid, self()),
    drain_spans(),
    Config.

request_is_traced_with_route_and_controller(_Config) ->
    {200, _} = request(get, "/hello", []),
    #span{name = Name, attributes = Attributes} = receive_span(),
    ?assertEqual(~"GET /hello", Name),
    ?assertMatch(
        #{
            'http.request.method' := ~"GET",
            'http.route' := ~"/hello",
            'http.response.status_code' := 200,
            'nova.app' := ~"nova_otel_demo",
            'nova.controller' := ~"demo_controller",
            'nova.action' := ~"hello"
        },
        otel_attributes:map(Attributes)
    ).

plugins_after_otel_still_run(_Config) ->
    Body = ~"{\"message\":\"hi\"}",
    {201, Response} = request(post, "/echo", Body),
    ?assertEqual(#{~"echo" => Body}, json:decode(Response)),
    ?assertMatch(#span{name = ~"POST /echo"}, receive_span()).

otel_plugin_runs_before_the_request_plugin(_Config) ->
    {400, _} = request(post, "/echo", ~"{not json"),
    #span{name = Name, attributes = Attributes} = receive_span(),
    ?assertEqual(~"POST /echo", Name),
    ?assertMatch(
        #{'http.response.status_code' := 400, 'nova.action' := ~"echo"},
        otel_attributes:map(Attributes)
    ).

incoming_trace_context_is_continued(_Config) ->
    TraceId = otel_id_generator:generate_trace_id(),
    SpanId = otel_id_generator:generate_span_id(),
    Traceparent = io_lib:format("00-~32.16.0b-~16.16.0b-01", [TraceId, SpanId]),
    {200, _} = request(get, "/hello", [{"traceparent", Traceparent}]),
    ?assertMatch(#span{trace_id = TraceId, parent_span_id = SpanId}, receive_span()).

prometheus_endpoint_counts_the_request(_Config) ->
    Before = hello_count(),
    {200, _} = request(get, "/hello", []),
    _ = receive_span(),
    ?assertEqual(Before + 1, await_hello_count(Before + 1, deadline())).

hello_count() ->
    ok = otel_meter_server:force_flush(),
    Url = "http://127.0.0.1:" ++ integer_to_list(?METRICS_PORT) ++ "/metrics",
    {200, Metrics} = response(httpc:request(get, {Url, []}, [], [{body_format, binary}])),
    {match, Lines} = re:run(
        Metrics,
        "^http_server_request_duration_seconds_count\\{([^}]*)\\} ([0-9]+)$",
        [multiline, global, {capture, all_but_first, binary}]
    ),
    Wanted = [~"http_route=\"/hello\"", ~"http_request_method=\"GET\"", ~"http_response_status_code=\"200\""],
    Counts = [
        binary_to_integer(Count)
     || [Labels, Count] <- Lines,
        is_binary(Labels),
        is_binary(Count),
        Wanted -- binary:split(Labels, ~",", [global]) =:= []
    ],
    lists:sum(Counts).

await_hello_count(Expected, Deadline) ->
    case hello_count() of
        Expected ->
            Expected;
        Count ->
            case erlang:monotonic_time(millisecond) < Deadline of
                true ->
                    timer:sleep(50),
                    await_hello_count(Expected, Deadline);
                false ->
                    Count
            end
    end.

request(get, Path, Headers) ->
    response(httpc:request(get, {url(Path), Headers}, [], [{body_format, binary}]));
request(post, Path, Body) ->
    response(
        httpc:request(post, {url(Path), [], "application/json", Body}, [], [{body_format, binary}])
    ).

url(Path) ->
    "http://127.0.0.1:" ++ integer_to_list(?HTTP_PORT) ++ Path.

response({ok, {{_, Status, _}, _Headers, Body}}) ->
    {Status, Body}.

receive_span() ->
    receive_span(deadline()).

receive_span(Deadline) ->
    receive
        {span, Span} -> Span
    after 50 ->
        case erlang:monotonic_time(millisecond) < Deadline of
            true ->
                ok = otel_tracer_provider:force_flush(),
                receive_span(Deadline);
            false ->
                ct:fail(timeout_waiting_for_span)
        end
    end.

drain_spans() ->
    receive
        {span, _} -> drain_spans()
    after 0 -> ok
    end.

deadline() ->
    erlang:monotonic_time(millisecond) + ?DEADLINE_MS.

load(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, App}} -> ok
    end.
