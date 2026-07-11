# Changelog

## 0.7.0

- Container now runs as a dedicated non-root user (uid 65532 `exporter`) instead of root.

## 0.6.1

- Add stdlib unittest suite covering metric-name sanitization, batching, stats parsing, catalog filtering, curated/full-catalog collection, config validation, and the landing page.

## 0.6.0

- Add `/healthz` (liveness) and `/readyz` (readiness) health check endpoints. `/healthz` always returns `200 ok` while the process serves; `/readyz` returns `200` with a small JSON body when at least one curated poll has succeeded and the data is fresh within `max(3 × POLL_INTERVAL, 90s)`, else `503`. Both are linked from the HTML landing page.

## 0.5.0

- Convert `print()`-based output to Python's `logging` module — messages now carry a timestamp and level (`INFO`/`WARNING`/`CRITICAL`). Fatal config/connectivity errors continue to go to stderr.
- Add `LOG_LEVEL` config (default `INFO`) to control log verbosity; validated at startup like other config.

## 0.4.0

- Add fail-fast startup validation: required config (`ONEFS_USERNAME`, `ONEFS_PASSWORD`, `ONEFS_ENDPOINT`, port/interval ranges) is checked before the process starts serving.
- Add a preflight connectivity check against the OneFS API at startup — exits immediately with a clear error on authentication failure or unreachable endpoint instead of starting up and failing silently on the first poll.

## 0.3.0

- Add an HTML landing page at `/` showing exporter status (last successful poll, poll intervals, full-catalog status) with a link to `/metrics`.

## 0.2.0

- Add full-catalog collection mode (`ALL_STATS_ENABLED`): polls every numeric key from `/platform/3/statistics/keys` (cluster + node scope) and exposes them as `onefs_raw_*` metrics on a separate, longer poll interval.

## 0.1.0

- Initial release: curated cluster capacity, cluster/node performance, per-protocol I/O, per-node network throughput, job engine, and health/alert metrics.
