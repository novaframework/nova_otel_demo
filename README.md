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

It listens on `http://127.0.0.1:8080` with three routes:

```
curl localhost:8080/hello
curl localhost:8080/slow
curl -X POST -H 'content-type: application/json' -d '{"message":"hi"}' localhost:8080/echo
```

`make simulate` sends random traffic to all three for two minutes.
`make down` stops the stack and removes its volumes.

## Where to look

- Grafana: <http://localhost:3000>, no login, read-only (anonymous Viewer).
  The **Nova OTel Demo** dashboard shows request rate, duration percentiles,
  active requests, body sizes, status codes and recent traces. Spans are named
  after the route (`GET /hello`) and carry `http.*` and `nova.*` attributes.
  Explore needs the Editor role: set `GF_AUTH_ANONYMOUS_ORG_ROLE=Editor` in
  `docker/docker-compose.yml` if you want it.
- Metrics, raw: `curl localhost:9464/metrics`.

Grafana and the collector's OTLP ports are published on `127.0.0.1` only;
Tempo and Mimir are reachable only inside the compose network.

## Wiring

- `config/dev_sys.config`: `otel_nova_stream_h` first in `stream_handlers`,
  `otel_nova_plugin` first among the `pre_request` plugins, the OTLP trace
  exporter and the metric reader that feeds `otel_nova_prom_exporter`.
  `trust_incoming_context` is `true` so a client's `traceparent` is
  continued; an application facing untrusted clients sets it to `false`.
- `src/nova_otel_demo_app.erl`: `ok = opentelemetry_nova:setup()` creates the
  metric instruments.
- `src/nova_otel_demo_sup.erl`: `otel_nova_prom_server` serves `/metrics` on
  `metrics_port` (9464), bound to `metrics_ip`, default `127.0.0.1`.
  `config/dev_sys.config` sets `{0, 0, 0, 0}` so the collector container can
  scrape it through `host.docker.internal`. That also exposes the
  unauthenticated endpoint to your LAN. In production, restrict who can
  connect, e.g. with a Kubernetes `NetworkPolicy` as in the
  [opentelemetry_nova README](https://github.com/novaframework/opentelemetry_nova#prometheus-metrics).

The demo reads request bodies without a size limit: `nova_request_plugin` in
Nova 0.18 has no limit option. A real application needs one, in a proxy in
front of it or in its own body reading.

## Test

```
rebar3 ct
```

The suite boots the application on ports 18080 and 19464, so it does not
collide with a running `rebar3 shell`, and checks spans and metrics for real
HTTP requests.
