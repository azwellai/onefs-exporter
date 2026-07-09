# Changelog

## 0.2.0

- Add full-catalog collection mode (`ALL_STATS_ENABLED`): polls every numeric key from `/platform/3/statistics/keys` (cluster + node scope) and exposes them as `onefs_raw_*` metrics on a separate, longer poll interval.

## 0.1.0

- Initial release: curated cluster capacity, cluster/node performance, per-protocol I/O, per-node network throughput, job engine, and health/alert metrics.
