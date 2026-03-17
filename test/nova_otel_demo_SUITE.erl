-module(nova_otel_demo_SUITE).
-behaviour(ct_suite).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").
-include_lib("opentelemetry/include/otel_span.hrl").
-include_lib("opentelemetry_experimental/include/otel_metrics.hrl").

-define(assertReceiveMetric(Name, Fun),
    (fun() ->
        receive
            {otel_metric, M = #metric{name = N}} when N =:= Name ->
                (Fun)(M)
        after 5000 ->
            ct:fail({timeout_waiting_for_metric, Name})
        end
    end)()).

%%--------------------------------------------------------------------
%% CT callbacks
%%--------------------------------------------------------------------

all() ->
    [span_created_on_request,
     span_captures_status_code,
     span_error_on_5xx,
     plugin_sets_controller_info,
     trace_context_propagation,
     metric_request_duration,
     metric_active_requests,
     metric_body_sizes,
     slow_endpoint_duration_variance].

init_per_suite(Config) ->
    application:ensure_all_started(opentelemetry),
    ok = application:set_env(opentelemetry_experimental, readers, [
        #{module => otel_metric_reader,
          config => #{exporter => {test_metric_exporter, []}}}
    ]),
    application:ensure_all_started(opentelemetry_experimental),
    opentelemetry_nova:setup(),
    Config.

end_per_suite(_Config) ->
    application:stop(opentelemetry_experimental),
    application:stop(opentelemetry),
    ok.

init_per_testcase(_TestCase, Config) ->
    otel_batch_processor:set_exporter(otel_exporter_pid, self()),
    persistent_term:put(test_metric_pid, self()),
    Config.

end_per_testcase(_TestCase, _Config) ->
    persistent_term:erase(test_metric_pid),
    otel_ctx:clear(),
    ok.

%%--------------------------------------------------------------------
%% Span tests
%%--------------------------------------------------------------------

span_created_on_request(_Config) ->
    Req = make_req(<<"GET">>, <<"/hello">>),
    {_Cmds, State} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"{\"message\":\"Hello!\"}">>}, State),
    otel_nova_stream_h:terminate(1, normal, State1),

    receive
        {span, Span = #span{name = Name}} ->
            ?assertEqual(<<"HTTP GET">>, Name),
            Attrs = otel_attributes:map(Span#span.attributes),
            ?assertEqual(<<"GET">>, maps:get('http.request.method', Attrs)),
            ?assertEqual(<<"/hello">>, maps:get('url.path', Attrs)),
            ?assertEqual(<<"http">>, maps:get('url.scheme', Attrs)),
            ?assertEqual(<<"localhost">>, maps:get('server.address', Attrs)),
            ?assertEqual(8080, maps:get('server.port', Attrs)),
            ?assertEqual(200, maps:get('http.response.status_code', Attrs))
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.

span_captures_status_code(_Config) ->
    Req = make_req(<<"POST">>, <<"/echo">>),
    {_Cmds, State0} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"{\"echo\":\"test\"}">>}, State0),
    otel_nova_stream_h:terminate(1, normal, State1),

    receive
        {span, Span = #span{}} ->
            Attrs = otel_attributes:map(Span#span.attributes),
            ?assertEqual(200, maps:get('http.response.status_code', Attrs))
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.

span_error_on_5xx(_Config) ->
    Req = make_req(<<"GET">>, <<"/boom">>),
    {_Cmds, State0} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 500, #{}, <<"Internal Server Error">>}, State0),
    otel_nova_stream_h:terminate(1, normal, State1),

    receive
        {span, #span{status = Status}} ->
            ?assertMatch(#status{code = ?OTEL_STATUS_ERROR}, Status)
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.

plugin_sets_controller_info(_Config) ->
    Req = make_req(<<"GET">>, <<"/hello">>),
    {_Cmds, State} = otel_nova_stream_h:init(1, Req, stream_opts()),

    Env = #{app => nova_otel_demo, callback => fun demo_controller:hello/1},
    {ok, _Req1, _PluginState} = otel_nova_plugin:pre_request(Req, Env, #{}, undefined),

    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"OK">>}, State),
    otel_nova_stream_h:terminate(1, normal, State1),

    receive
        {span, Span = #span{name = Name}} ->
            ?assertEqual(<<"GET /hello">>, Name),
            Attrs = otel_attributes:map(Span#span.attributes),
            ?assertEqual(<<"nova_otel_demo">>, maps:get('nova.app', Attrs)),
            ?assertEqual(<<"demo_controller">>, maps:get('nova.controller', Attrs)),
            ?assertEqual(<<"hello">>, maps:get('nova.action', Attrs))
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.

trace_context_propagation(_Config) ->
    TraceId = otel_id_generator:generate_trace_id(),
    SpanId = otel_id_generator:generate_span_id(),
    TraceIdHex = list_to_binary(io_lib:format("~32.16.0b", [TraceId])),
    SpanIdHex = list_to_binary(io_lib:format("~16.16.0b", [SpanId])),
    Traceparent = <<"00-", TraceIdHex/binary, "-", SpanIdHex/binary, "-01">>,

    Req = make_req(<<"GET">>, <<"/hello">>, #{<<"traceparent">> => Traceparent}),
    {_Cmds, State} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"OK">>}, State),
    otel_nova_stream_h:terminate(1, normal, State1),

    receive
        {span, #span{trace_id = RecvTraceId, parent_span_id = ParentSpanId}} ->
            ?assertEqual(TraceId, RecvTraceId),
            ?assertEqual(SpanId, ParentSpanId)
    after 5000 ->
        ct:fail(timeout_waiting_for_span)
    end.

%%--------------------------------------------------------------------
%% Metric tests
%%--------------------------------------------------------------------

metric_request_duration(_Config) ->
    Req = make_req(<<"GET">>, <<"/hello">>),
    {_Cmds, State0} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"OK">>}, State0),
    otel_nova_stream_h:terminate(1, normal, State1),
    drain_spans(),

    otel_meter_server:force_flush(),

    ?assertReceiveMetric('http.server.request.duration', fun(#metric{data = Data}) ->
        #histogram{datapoints = Datapoints} = Data,
        ?assertMatch([_ | _], Datapoints),
        DP = find_datapoint(#{'http.response.status_code' => 200,
                              'http.request.method' => <<"GET">>}, Datapoints),
        ?assert(DP#histogram_datapoint.sum >= 0),
        ?assert(DP#histogram_datapoint.count >= 1),
        Attrs = DP#histogram_datapoint.attributes,
        ?assertEqual(<<"http">>, maps:get('url.scheme', Attrs)),
        ?assertEqual(<<"localhost">>, maps:get('server.address', Attrs)),
        ?assertEqual(8080, maps:get('server.port', Attrs))
    end).

metric_active_requests(_Config) ->
    Req = make_req(<<"GET">>, <<"/hello">>),
    {_Cmds, State0} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"OK">>}, State0),
    otel_nova_stream_h:terminate(1, normal, State1),
    drain_spans(),

    otel_meter_server:force_flush(),

    ?assertReceiveMetric('http.server.active_requests', fun(#metric{data = Data}) ->
        #sum{datapoints = Datapoints, is_monotonic = false} = Data,
        ?assertMatch([_ | _], Datapoints),
        [DP | _] = Datapoints,
        ?assertEqual(0, DP#datapoint.value)
    end).

metric_body_sizes(_Config) ->
    Req = make_req(<<"POST">>, <<"/echo">>),
    ReqBody = <<"{\"msg\":\"hello\"}">>,
    RespBody = <<"{\"echo\":{\"msg\":\"hello\"}}">>,

    {_Cmds, State0} = otel_nova_stream_h:init(1, Req, stream_opts()),
    {_Cmds1, State1} = otel_nova_stream_h:data(1, fin, ReqBody, State0),
    {_Cmds2, State2} = otel_nova_stream_h:info(1, {response, 200, #{}, RespBody}, State1),
    otel_nova_stream_h:terminate(1, normal, State2),
    drain_spans(),

    otel_meter_server:force_flush(),

    ?assertReceiveMetric('http.server.request.body.size', fun(#metric{data = Data}) ->
        #histogram{datapoints = Datapoints} = Data,
        [DP | _] = Datapoints,
        ?assertEqual(byte_size(ReqBody), DP#histogram_datapoint.sum),
        ?assert(DP#histogram_datapoint.count >= 1)
    end),

    ?assertReceiveMetric('http.server.response.body.size', fun(#metric{data = Data}) ->
        #histogram{datapoints = Datapoints} = Data,
        [DP | _] = Datapoints,
        ?assertEqual(byte_size(RespBody), DP#histogram_datapoint.sum),
        ?assert(DP#histogram_datapoint.count >= 1)
    end).

slow_endpoint_duration_variance(_Config) ->
    %% Simulate the /slow endpoint: two requests with different sleep times
    %% should produce different durations in metrics
    Req1 = make_req(<<"GET">>, <<"/slow">>),
    {_Cmds, State0} = otel_nova_stream_h:init(1, Req1, stream_opts()),
    timer:sleep(100),
    {_Cmds1, State1} = otel_nova_stream_h:info(1, {response, 200, #{}, <<"OK">>}, State0),
    otel_nova_stream_h:terminate(1, normal, State1),

    Req2 = make_req(<<"GET">>, <<"/slow">>),
    {_Cmds2, State2} = otel_nova_stream_h:init(2, Req2, stream_opts()),
    timer:sleep(300),
    {_Cmds3, State3} = otel_nova_stream_h:info(2, {response, 200, #{}, <<"OK">>}, State2),
    otel_nova_stream_h:terminate(2, normal, State3),
    drain_spans(),

    otel_meter_server:force_flush(),

    ?assertReceiveMetric('http.server.request.duration', fun(#metric{data = Data}) ->
        #histogram{datapoints = Datapoints} = Data,
        DP = find_datapoint(#{'http.response.status_code' => 200,
                              'http.request.method' => <<"GET">>}, Datapoints),
        %% Two requests, total duration > 0.4s (100ms + 300ms)
        ?assert(DP#histogram_datapoint.count >= 2),
        ?assert(DP#histogram_datapoint.sum >= 0.4)
    end).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

stream_opts() ->
    #{stream_handlers => []}.

make_req(Method, Path) ->
    make_req(Method, Path, #{}).

make_req(Method, Path, ExtraHeaders) ->
    BaseHeaders = #{<<"host">> => <<"localhost:8080">>,
                    <<"user-agent">> => <<"test-agent/1.0">>},
    Headers = maps:merge(BaseHeaders, ExtraHeaders),
    #{method => Method,
      path => Path,
      scheme => <<"http">>,
      host => <<"localhost">>,
      port => 8080,
      qs => <<>>,
      version => 'HTTP/1.1',
      headers => Headers,
      peer => {{127, 0, 0, 1}, 12345},
      cert => undefined,
      pid => self(),
      streamid => 1}.

drain_spans() ->
    receive {span, _} -> drain_spans()
    after 100 -> ok
    end.

find_datapoint(Match, [DP = #histogram_datapoint{attributes = Attrs} | Rest]) ->
    case maps:with(maps:keys(Match), Attrs) of
        Match -> DP;
        _ -> find_datapoint(Match, Rest)
    end;
find_datapoint(Match, []) ->
    ct:fail({no_datapoint_matching, Match}).
