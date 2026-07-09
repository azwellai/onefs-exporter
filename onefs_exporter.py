#!/usr/bin/env python3
import base64
import json
import os
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

ENDPOINT = os.environ.get("ONEFS_ENDPOINT", "onefs.example.com:8080")
USERNAME = os.environ.get("ONEFS_USERNAME", "")
PASSWORD = os.environ.get("ONEFS_PASSWORD", "")
INSECURE = os.environ.get("ONEFS_INSECURE", "true").lower() == "true"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
ALL_POLL_INTERVAL = int(os.environ.get("ALL_POLL_INTERVAL_SECONDS", "300"))
ALL_BATCH_SIZE = int(os.environ.get("ALL_BATCH_SIZE", "200"))
ALL_STATS_ENABLED = os.environ.get("ALL_STATS_ENABLED", "true").lower() == "true"
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9684"))
TIMEOUT = int(os.environ.get("ONEFS_API_TIMEOUT", "10"))

NUMERIC_TYPES = {"uint64", "int32", "double", "int64"}

AUTH_HEADER = "Basic " + base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
SSL_CTX = ssl._create_unverified_context() if INSECURE else ssl.create_default_context()

PROTOCOLS = ["nfs", "nfs4", "smb2", "http"]

STAT_KEYS_CLUSTER = [
    "ifs.bytes.total",
    "ifs.bytes.avail",
    "cluster.health",
    "cluster.alert.info",
    "cluster.cpu.sys.avg",
    "cluster.disk.xfers.in.rate",
    "cluster.disk.xfers.out.rate",
    "cluster.disk.bytes.in.rate",
    "cluster.disk.bytes.out.rate",
] + [f"cluster.protostats.{p}.total" for p in PROTOCOLS]

STAT_KEYS_NODE = [
    "node.health",
    "node.cpu.idle.avg",
    "node.cpu.sys.avg",
    "node.cpu.user.avg",
    "node.memory.used",
    "node.memory.free",
    "node.net.ext.bytes.in.rate",
    "node.net.ext.bytes.out.rate",
    "node.net.int.bytes.in.rate",
    "node.net.int.bytes.out.rate",
]

_lock = threading.Lock()
_cache_text = "# no data collected yet\n"
_last_success = 0
_last_error = ""

_all_lock = threading.Lock()
_all_cache_text = "# no full-catalog data collected yet\n"
_all_last_error = ""
_all_last_duration = 0.0
_all_catalog_cluster_keys = []
_all_catalog_node_keys = []


def sanitize_metric_name(key):
    return "onefs_raw_" + "".join(c if (c.isalnum() or c == "_") else "_" for c in key)


def fetch_catalog():
    data = onefs_get("/platform/3/statistics/keys")
    cluster_keys, node_keys = [], []
    for k in data.get("keys", []):
        if k.get("type") not in NUMERIC_TYPES:
            continue
        if k.get("scope") == "cluster":
            cluster_keys.append(k["key"])
        elif k.get("scope") == "node":
            node_keys.append(k["key"])
    return cluster_keys, node_keys


def chunked(seq, size):
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def collect_all():
    global _all_catalog_cluster_keys, _all_catalog_node_keys
    if not _all_catalog_cluster_keys and not _all_catalog_node_keys:
        _all_catalog_cluster_keys, _all_catalog_node_keys = fetch_catalog()
        print(
            f"[onefs-exporter] full catalog loaded: "
            f"{len(_all_catalog_cluster_keys)} cluster keys, "
            f"{len(_all_catalog_node_keys)} node keys (numeric only)",
            flush=True,
        )

    lines = []
    emitted_help = set()

    def emit(key, devid, value):
        name = sanitize_metric_name(key)
        if name not in emitted_help:
            lines.append(f"# HELP {name} raw OneFS statistics key {key}")
            lines.append(f"# TYPE {name} gauge")
            emitted_help.add(name)
        if devid:
            lines.append(f'{name}{{node="{devid}"}} {value}')
        else:
            lines.append(f"{name} {value}")

    for batch in chunked(_all_catalog_cluster_keys, ALL_BATCH_SIZE):
        try:
            stats = fetch_stats(batch)
        except Exception as e:
            print(f"[onefs-exporter] cluster batch failed: {e}", flush=True)
            continue
        for key, entries in stats.items():
            for s in entries:
                v = s.get("value")
                if isinstance(v, (int, float)):
                    emit(key, None, v)

    for batch in chunked(_all_catalog_node_keys, ALL_BATCH_SIZE):
        try:
            stats = fetch_stats(batch, nodes_all=True)
        except Exception as e:
            print(f"[onefs-exporter] node batch failed: {e}", flush=True)
            continue
        for key, entries in stats.items():
            for s in entries:
                v = s.get("value")
                if isinstance(v, (int, float)):
                    emit(key, s.get("devid"), v)

    return "\n".join(lines) + "\n"


def onefs_get(path, params=None):
    query = ("?" + urllib.parse.urlencode(params)) if params else ""
    url = f"https://{ENDPOINT}{path}{query}"
    req = urllib.request.Request(url, headers={"Authorization": AUTH_HEADER})
    with urllib.request.urlopen(req, timeout=TIMEOUT, context=SSL_CTX) as resp:
        return json.loads(resp.read())


def fetch_stats(keys, nodes_all=False):
    params = {"keys": ",".join(keys)}
    if nodes_all:
        params["nodes"] = "all"
    data = onefs_get("/platform/3/statistics/current", params)
    by_key = {}
    for stat in data.get("stats", []):
        if stat.get("error"):
            continue
        by_key.setdefault(stat["key"], []).append(stat)
    return by_key


def fetch_running_jobs():
    data = onefs_get("/platform/1/job/jobs", {"state": "running"})
    return data.get("jobs", [])


def esc(label_value):
    return str(label_value).replace("\\", "\\\\").replace('"', '\\"')


def collect():
    lines = []

    def gauge(name, help_text):
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} gauge")

    # cluster-wide stats
    cluster_stats = fetch_stats(STAT_KEYS_CLUSTER)

    def cluster_val(key):
        s = cluster_stats.get(key)
        return s[0]["value"] if s else None

    gauge("onefs_cluster_capacity_total_bytes", "Total cluster capacity in bytes")
    v = cluster_val("ifs.bytes.total")
    if v is not None:
        lines.append(f"onefs_cluster_capacity_total_bytes {v}")

    gauge("onefs_cluster_capacity_avail_bytes", "Available cluster capacity in bytes")
    v = cluster_val("ifs.bytes.avail")
    if v is not None:
        lines.append(f"onefs_cluster_capacity_avail_bytes {v}")

    gauge("onefs_cluster_health", "Cluster health: 0=Healthy 1=Attention 2=Down")
    v = cluster_val("cluster.health")
    if v is not None:
        lines.append(f"onefs_cluster_health {v}")

    gauge("onefs_cluster_alert_count", "Number of active critical-and-above alerts")
    v = cluster_val("cluster.alert.info")
    if v is not None:
        lines.append(f"onefs_cluster_alert_count {len(v)}")

    gauge("onefs_cluster_cpu_sys_percent", "Cluster average system CPU percent (x10)")
    v = cluster_val("cluster.cpu.sys.avg")
    if v is not None:
        lines.append(f"onefs_cluster_cpu_sys_percent {v / 10.0}")

    for metric, key in [
        ("onefs_cluster_disk_xfers_in_rate", "cluster.disk.xfers.in.rate"),
        ("onefs_cluster_disk_xfers_out_rate", "cluster.disk.xfers.out.rate"),
        ("onefs_cluster_disk_bytes_in_rate", "cluster.disk.bytes.in.rate"),
        ("onefs_cluster_disk_bytes_out_rate", "cluster.disk.bytes.out.rate"),
    ]:
        gauge(metric, f"OneFS stat key {key}")
        v = cluster_val(key)
        if v is not None:
            lines.append(f"{metric} {v}")

    gauge("onefs_protocol_op_rate", "Protocol operation rate (ops/sec)")
    gauge("onefs_protocol_in_rate_bytes", "Protocol inbound byte rate")
    gauge("onefs_protocol_out_rate_bytes", "Protocol outbound byte rate")
    for proto in PROTOCOLS:
        s = cluster_stats.get(f"cluster.protostats.{proto}.total")
        if not s or not s[0]["value"]:
            continue
        entry = s[0]["value"][0]
        lbl = f'protocol="{esc(proto)}"'
        lines.append(f"onefs_protocol_op_rate{{{lbl}}} {entry.get('op_rate', 0)}")
        lines.append(f"onefs_protocol_in_rate_bytes{{{lbl}}} {entry.get('in_rate', 0)}")
        lines.append(f"onefs_protocol_out_rate_bytes{{{lbl}}} {entry.get('out_rate', 0)}")

    # per-node stats
    node_stats = fetch_stats(STAT_KEYS_NODE, nodes_all=True)

    def node_values(key):
        return {str(s["devid"]): s["value"] for s in node_stats.get(key, [])}

    gauge("onefs_node_health", "Node health: 0=Healthy 1=Attention 2=Down")
    for node, v in node_values("node.health").items():
        lines.append(f'onefs_node_health{{node="{node}"}} {v}')

    for metric, key, scale in [
        ("onefs_node_cpu_idle_percent", "node.cpu.idle.avg", 0.1),
        ("onefs_node_cpu_sys_percent", "node.cpu.sys.avg", 0.1),
        ("onefs_node_cpu_user_percent", "node.cpu.user.avg", 0.1),
        ("onefs_node_memory_used_bytes", "node.memory.used", 1),
        ("onefs_node_memory_free_bytes", "node.memory.free", 1),
        ("onefs_node_net_ext_bytes_in_rate", "node.net.ext.bytes.in.rate", 1),
        ("onefs_node_net_ext_bytes_out_rate", "node.net.ext.bytes.out.rate", 1),
        ("onefs_node_net_int_bytes_in_rate", "node.net.int.bytes.in.rate", 1),
        ("onefs_node_net_int_bytes_out_rate", "node.net.int.bytes.out.rate", 1),
    ]:
        gauge(metric, f"OneFS stat key {key}")
        for node, v in node_values(key).items():
            lines.append(f'{metric}{{node="{node}"}} {v * scale}')

    # job engine
    gauge("onefs_job_engine_running_jobs", "Number of currently running job engine jobs")
    try:
        jobs = fetch_running_jobs()
        lines.append(f"onefs_job_engine_running_jobs {len(jobs)}")
    except Exception:
        pass

    gauge("onefs_exporter_scrape_success", "1 if the last scrape of OneFS succeeded")
    lines.append("onefs_exporter_scrape_success 1")
    gauge("onefs_exporter_last_success_timestamp_seconds", "Unix time of last successful scrape")
    lines.append(f"onefs_exporter_last_success_timestamp_seconds {int(time.time())}")

    return "\n".join(lines) + "\n"


def poll_loop():
    global _cache_text, _last_success, _last_error
    while True:
        try:
            text = collect()
            with _lock:
                _cache_text = text
                _last_success = time.time()
                _last_error = ""
        except Exception as e:
            with _lock:
                _last_error = str(e)
            print(f"[onefs-exporter] scrape failed: {e}", flush=True)
        time.sleep(POLL_INTERVAL)


def poll_loop_all():
    global _all_cache_text, _all_last_error, _all_last_duration
    if not ALL_STATS_ENABLED:
        return
    while True:
        start = time.time()
        try:
            text = collect_all()
            dur = time.time() - start
            with _all_lock:
                _all_cache_text = text
                _all_last_error = ""
                _all_last_duration = dur
            print(f"[onefs-exporter] full-catalog sweep done in {dur:.1f}s, "
                  f"{text.count(chr(10))} lines", flush=True)
        except Exception as e:
            with _all_lock:
                _all_last_error = str(e)
            print(f"[onefs-exporter] full-catalog sweep failed: {e}", flush=True)
        time.sleep(max(ALL_POLL_INTERVAL - (time.time() - start), 5))


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.startswith("/metrics"):
            with _lock:
                body = _cache_text
                err = _last_error
            if err:
                body += f'\nonefs_exporter_scrape_success 0\n# last_error: {err}\n'
            with _all_lock:
                body += "\n" + _all_cache_text
                all_err = _all_last_error
                all_dur = _all_last_duration
            body += (
                "# HELP onefs_exporter_all_stats_sweep_duration_seconds Duration of last full-catalog sweep\n"
                "# TYPE onefs_exporter_all_stats_sweep_duration_seconds gauge\n"
                f"onefs_exporter_all_stats_sweep_duration_seconds {all_dur}\n"
            )
            if all_err:
                body += f"# last_all_stats_error: {all_err}\n"
            data = body.encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        pass


if __name__ == "__main__":
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=poll_loop_all, daemon=True).start()
    print(f"[onefs-exporter] listening on :{LISTEN_PORT}, polling {ENDPOINT} "
          f"every {POLL_INTERVAL}s (curated) / {ALL_POLL_INTERVAL}s (full catalog)", flush=True)
    ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()
