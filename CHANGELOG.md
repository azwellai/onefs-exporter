# Changelog

## 0.4.0

- Add fail-fast startup validation: required config (`ONEFS_USERNAME`, `ONEFS_PASSWORD`, `ONEFS_ENDPOINT`, port/interval ranges) is checked before the process starts serving.
- Add a preflight connectivity check against the OneFS API at startup — exits immediately with a clear error on authentication failure or unreachable endpoint instead of starting up and failing silently on the first poll.

## 0.3.0

- Add an HTML landing page at `/` showing exporter status (last successful poll, poll intervals, full-catalog status) with a link to `/metrics`.

## 0.2.0

- Add full-catalog collection mode (`ALL_STATS_ENABLED`): polls every numeric key from `/platform/3/statistics/keys` (cluster + node scope) and exposes them as `onefs_raw_*` metrics on a separate, longer poll interval.

## 0.1.0

- Initial release: curated cluster capacity, cluster/node performance, per-protocol I/O, per-node network throughput, job engine, and health/alert metrics.
