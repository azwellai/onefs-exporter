# onefs-exporter

A lightweight Prometheus exporter for Dell PowerScale (OneFS) clusters. It polls the OneFS REST API (PAPI) directly and exposes the results on `/metrics`. Written entirely with the Python standard library — no third-party dependencies.

## Why this project

Dell's official [csm-metrics-powerscale](https://github.com/dell/csm-metrics-powerscale) only exposes Kubernetes CSI volume-oriented capacity/performance metrics (just 10 metrics total) and only runs inside a Kubernetes cluster — leader election is hardcoded to use in-cluster config, so it cannot run as a standalone container. If you need broader operational metrics — per-node CPU/memory, per-protocol (NFS/SMB/HTTP) I/O, per-interface network throughput, job engine status, hardware alerts/health — use this project instead.

- Runs anywhere as a single container, no Kubernetes required
- Talks directly to the OneFS `/platform/3/statistics/current` API (access to 10,000+ statistics keys)
- Standard Prometheus text exposition format

## Architecture

```
┌─────────────────┐      REST/HTTPS       ┌──────────────────┐      /metrics       ┌────────────┐
│  PowerScale     │ <──────────────────── │  onefs-exporter  │ <────────────────── │ Prometheus │
│  (OneFS PAPI)   │      basic auth       │  (single binary) │   text exposition   │            │
└─────────────────┘                       └──────────────────┘                     └────────────┘
```

- **Curated metrics**: the commonly-needed core metrics (capacity/performance, per-node CPU & memory, protocol, network, job engine, health) polled every 30s
- **Full-catalog metrics** (optional): every numeric statistics key OneFS exposes (~8,000 keys, `onefs_raw_*` prefix) polled every 5 minutes — useful for discovery/exploration

## Requirements

- A PowerScale OneFS cluster with REST API access (default port 8080)
- An account with read permissions (statistics/events/job list)
- A container runtime (Docker or nerdctl/containerd)

## Quick start

### 1. Build the image

```bash
docker build -t onefs-exporter:latest .
# or with nerdctl
nerdctl build -t onefs-exporter:latest .
```

### 2. Configure environment variables

Copy `deploy/env.example` and fill in real values.

```bash
cp deploy/env.example deploy/env
vi deploy/env
```

### 3. Run

```bash
docker run -d --name onefs-exporter \
  --env-file deploy/env \
  -p 9684:9684 \
  --restart unless-stopped \
  onefs-exporter:latest
```

### 4. Verify

```bash
curl http://localhost:9684/metrics
```

## Running under systemd (no Docker/nerdctl daemon)

Use `deploy/onefs-exporter.service.example` as a template for `/etc/systemd/system/onefs-exporter.service`.

```bash
cp deploy/onefs-exporter.service.example /etc/systemd/system/onefs-exporter.service
cp deploy/env.example /etc/onefs-exporter/env   # then fill in real values
systemctl daemon-reload
systemctl enable --now onefs-exporter.service
```

## Configuration (environment variables)

| Variable | Default | Description |
|---|---|---|
| `ONEFS_ENDPOINT` | `onefs.example.com:8080` | OneFS API endpoint (`host:port`) |
| `ONEFS_USERNAME` | (none) | OneFS account username |
| `ONEFS_PASSWORD` | (none) | OneFS account password |
| `ONEFS_INSECURE` | `true` | If `true`, skip TLS certificate verification (for self-signed certs) |
| `ONEFS_API_TIMEOUT` | `10` | API call timeout in seconds |
| `POLL_INTERVAL_SECONDS` | `30` | Poll interval for curated metrics (seconds) |
| `ALL_STATS_ENABLED` | `true` | Whether to collect full-catalog metrics |
| `ALL_POLL_INTERVAL_SECONDS` | `300` | Poll interval for full-catalog metrics (seconds) |
| `ALL_BATCH_SIZE` | `200` | Number of keys per API call when fetching the full catalog |
| `LISTEN_PORT` | `9684` | Port the exporter listens on |
| `LOG_LEVEL` | `INFO` | Log verbosity — DEBUG/INFO/WARNING/ERROR/CRITICAL |

## Metrics

### Curated metrics (`onefs_*`, always on)

| Metric | Description |
|---|---|
| `onefs_cluster_capacity_total_bytes` / `_avail_bytes` | Total / available cluster capacity |
| `onefs_cluster_health` | Cluster health: 0=Healthy, 1=Attention, 2=Down |
| `onefs_cluster_alert_count` | Number of active critical-and-above alerts |
| `onefs_cluster_cpu_sys_percent` | Cluster average system CPU % |
| `onefs_cluster_disk_xfers_in/out_rate`, `_bytes_in/out_rate` | Cluster disk transfer rates |
| `onefs_protocol_op_rate{protocol}` / `_in_rate_bytes` / `_out_rate_bytes` | Per-protocol (nfs/nfs4/smb2/http) throughput |
| `onefs_node_health{node}` | Per-node health |
| `onefs_node_cpu_idle/sys/user_percent{node}` | Per-node CPU usage |
| `onefs_node_memory_used/free_bytes{node}` | Per-node memory |
| `onefs_node_net_ext/int_bytes_in/out_rate{node}` | Per-node network throughput (external/internal) |
| `onefs_job_engine_running_jobs` | Number of currently running job engine jobs |
| `onefs_exporter_scrape_success` / `_last_success_timestamp_seconds` | Exporter's own health status |

### Full-catalog metrics (`onefs_raw_*`, when `ALL_STATS_ENABLED=true`)

Queries every numeric key (uint64/int32/double/int64) from OneFS's `/platform/3/statistics/keys` and exposes it as `onefs_raw_<key>` (roughly 8,000 keys, covering both cluster and node scope). String-typed keys and complex object types such as protostats are currently excluded.

> Since the payload is large (~2MB per scrape), it's recommended to inspect what you need, then set `ALL_STATS_ENABLED=false` and add the specific keys you care about to the curated list instead.

## Health endpoints

- `GET /healthz` (liveness) — always returns `200 ok` while the process is serving, independent of OneFS state.
- `GET /readyz` (readiness) — returns `200` when at least one curated poll has succeeded and the data is fresh (age within `max(3 × POLL_INTERVAL_SECONDS, 90s)`), otherwise `503`. The JSON body includes `ready`, `last_success_unix`, `age_seconds`, `last_error`, and `stale_threshold_seconds`.

## Example Prometheus config

```yaml
scrape_configs:
  - job_name: 'onefs-powerscale'
    scrape_interval: 30s
    static_configs:
      - targets: ['<exporter-host>:9684']
```

## Notes

- Authentication is HTTP Basic Auth, sent on every request.
- If the PowerScale cluster is a shared resource, don't set the full-catalog poll interval (`ALL_POLL_INTERVAL_SECONDS`) too aggressively.
- You can query the full OneFS statistics key catalog yourself via `GET /platform/3/statistics/keys`.

## Running tests

The test suite uses only the Python standard library (`unittest`) — there are no test dependencies to install. From the repository root:

```bash
python3 -m unittest -v
```

## License

MIT License — see [LICENSE](LICENSE)
