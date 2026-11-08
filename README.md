# nova_otel_demo

A small [Nova](https://github.com/novaframework/nova) application instrumented
with [opentelemetry_nova](https://github.com/novaframework/opentelemetry_nova),
plus a local Grafana stack to look at the result: an OpenTelemetry Collector,
Tempo for traces and Mimir for metrics.

Traces go over OTLP/HTTP to the collector on `localhost:4318`. Metrics are
served in Prometheus format on `:9464/metrics`, which the collector scrapes.

## Run

Start the observability stack:

```
docker compose -f docker/docker-compose.yml up -d
```

(`make up` does the same.) Then start the application:

```
rebar3 shell
```

It listens on `http://localhost:8080` with three routes:

```
curl localhost:8080/hello
curl localhost:8080/slow
curl -X POST -H 'content-type: application/json' -d '{"message":"hi"}' localhost:8080/echo
```

`make simulate` sends random traffic to all three for two minutes.
`make down` stops the stack and removes its volumes.

## Where to look

- Grafana: <http://localhost:3000>, no login. The **Nova OTel Demo** dashboard
  shows request rate, duration percentiles, active requests, body sizes,
  status codes and recent traces.
- Traces: Grafana, **Explore**, data source **Tempo**. Spans are named after
  the route (`GET /hello`) and carry `http.*` and `nova.*` attributes.
- Metrics: Grafana, **Explore**, data source **Mimir**, e.g.
  `rate(http_server_request_duration_seconds_count[1m])`, or the raw
  endpoint with `curl localhost:9464/metrics`.

## Wiring

- `config/dev_sys.config`: `otel_nova_stream_h` first in `stream_handlers`,
  `otel_nova_plugin` first among the `pre_request` plugins, the OTLP trace
  exporter and the metric reader that feeds `otel_nova_prom_exporter`.
- `src/nova_otel_demo_app.erl`: `ok = opentelemetry_nova:setup()` creates the
  metric instruments.
- `src/nova_otel_demo_sup.erl`: `otel_nova_prom_server` serves `/metrics` on
  port 9464, bound to all interfaces so the collector container can reach it
  through `host.docker.internal`. Do not expose it like that in production
  without restricting who can connect.

## Test

```
rebar3 ct
```

The suite boots the application and checks spans and metrics for real HTTP
requests.
