-module(nova_otel_demo_SUITE).
-behaviour(ct_suite).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("opentelemetry/include/otel_span.hrl").

-export([all/0, init_per_suite/1, end_per_suite/1, init_per_testcase/2]).
-export([
    request_is_traced_with_route_and_controller/1,
    plugins_after_otel_still_run/1,
    incoming_trace_context_is_continued/1,
    prometheus_endpoint_serves_route_metrics/1
]).

all() ->
    [
        request_is_traced_with_route_and_controller,
        plugins_after_otel_still_run,
        incoming_trace_context_is_continued,
        prometheus_endpoint_serves_route_metrics
    ].

init_per_suite(Config) ->
    ok = application:set_env(opentelemetry, traces_exporter, none),
    {ok, _} = application:ensure_all_started(nova_otel_demo),
    Config.

end_per_suite(_Config) ->
    [application:stop(App) || App <- [nova_otel_demo, opentelemetry_experimental, opentelemetry]],
    ok.

init_per_testcase(_TestCase, Config) ->
    otel_batch_processor:set_exporter(otel_exporter_pid, self()),
    Config.

request_is_traced_with_route_and_controller(_Config) ->
    {200, _} = request(get, "http://localhost:8080/hello", []),
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
    {201, Response} = request(post, "http://localhost:8080/echo", Body),
    ?assertEqual(#{~"echo" => Body}, json:decode(Response)),
    #span{name = Name} = receive_span(),
    ?assertEqual(~"POST /echo", Name).

incoming_trace_context_is_continued(_Config) ->
    TraceId = otel_id_generator:generate_trace_id(),
    SpanId = otel_id_generator:generate_span_id(),
    Traceparent = io_lib:format("00-~32.16.0b-~16.16.0b-01", [TraceId, SpanId]),
    {200, _} = request(get, "http://localhost:8080/hello", [{"traceparent", Traceparent}]),
    #span{trace_id = ReceivedTraceId, parent_span_id = ParentSpanId} = receive_span(),
    ?assertEqual(TraceId, ReceivedTraceId),
    ?assertEqual(SpanId, ParentSpanId).

prometheus_endpoint_serves_route_metrics(_Config) ->
    {200, _} = request(get, "http://localhost:8080/hello", []),
    _ = receive_span(),
    ok = otel_meter_server:force_flush(),
    {200, Metrics} = request(get, "http://127.0.0.1:9464/metrics", []),
    [
        ?assertNotEqual(nomatch, string:find(Metrics, Expected), Expected)
     || Expected <- [
            ~"http_server_request_duration_seconds_count{",
            ~"http_route=\"/hello\"",
            ~"http_server_active_requests{"
        ]
    ].

request(get, Url, Headers) ->
    response(httpc:request(get, {Url, Headers}, [], [{body_format, binary}]));
request(post, Url, Body) ->
    response(httpc:request(post, {Url, [], "application/json", Body}, [], [{body_format, binary}])).

response({ok, {{_, Status, _}, _Headers, Body}}) ->
    {Status, Body}.

receive_span() ->
    ok = otel_tracer_provider:force_flush(),
    receive
        {span, Span} -> Span
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.
