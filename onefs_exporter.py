#!/usr/bin/env python3
import base64
import json
import logging
import os
import ssl
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

def _resolve_password(env_password, env_password_file):
    """Resolve the OneFS password, preferring a file over the env var.

    If ONEFS_PASSWORD_FILE is set, the password is read from that path and only
    a trailing newline is stripped (other whitespace may legitimately be part of
    the password). Import must never crash on a bad file: on a read failure or an
    empty file we return "" plus a descriptive, path-specific error string so
    validate_config() can emit the fatal error at startup. Returns
    (password, error) where error is "" on success.
    """
    if env_password_file:
        try:
            with open(env_password_file) as f:
                pw = f.read().rstrip("\n")
        except OSError as e:
            return "", f"ONEFS_PASSWORD_FILE '{env_password_file}' could not be read: {e}"
        if not pw:
            return "", f"ONEFS_PASSWORD_FILE '{env_password_file}' is set but empty"
        return pw, ""
    return env_password, ""


ENDPOINT = os.environ.get("ONEFS_ENDPOINT", "onefs.example.com:8080")
USERNAME = os.environ.get("ONEFS_USERNAME", "")
PASSWORD_FILE = os.environ.get("ONEFS_PASSWORD_FILE", "")
PASSWORD, _password_file_error = _resolve_password(
    os.environ.get("ONEFS_PASSWORD", ""), PASSWORD_FILE
)
INSECURE = os.environ.get("ONEFS_INSECURE", "true").lower() == "true"
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL_SECONDS", "30"))
ALL_POLL_INTERVAL = int(os.environ.get("ALL_POLL_INTERVAL_SECONDS", "300"))
ALL_BATCH_SIZE = int(os.environ.get("ALL_BATCH_SIZE", "200"))
ALL_STATS_ENABLED = os.environ.get("ALL_STATS_ENABLED", "true").lower() == "true"
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "9684"))
TIMEOUT = int(os.environ.get("ONEFS_API_TIMEOUT", "10"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s [onefs-exporter] %(message)s",
)
logger = logging.getLogger("onefs-exporter")

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
        logger.info(
            "full catalog loaded: %d cluster keys, %d node keys (numeric only)",
            len(_all_catalog_cluster_keys),
            len(_all_catalog_node_keys),
        )

    lines = []
    emitted_help = set()

    def emit(key, devid, value):
        name = sanitize_metric_name(key)
        if name not in emitted_help:
            lines.append(f"# HELP {name} raw OneFS statistics key {key}")
            lines.append(f"# TYPE {name} gauge")
            emitted_help.add(name)
        if devid is not None:
            lines.append(f'{name}{{node="{devid}"}} {value}')
        else:
            lines.append(f"{name} {value}")

    for batch in chunked(_all_catalog_cluster_keys, ALL_BATCH_SIZE):
        try:
            stats = fetch_stats(batch)
        except Exception as e:
            logger.warning("cluster batch failed: %s", e)
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
            logger.warning("node batch failed: %s", e)
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


def validate_config():
    errors = []
    if not ENDPOINT:
        errors.append("ONEFS_ENDPOINT is required")
    if not USERNAME:
        errors.append("ONEFS_USERNAME is required")
    if _password_file_error:
        errors.append(_password_file_error)
    elif not PASSWORD:
        errors.append("ONEFS_PASSWORD or ONEFS_PASSWORD_FILE is required")
    if POLL_INTERVAL <= 0:
        errors.append("POLL_INTERVAL_SECONDS must be a positive integer")
    if TIMEOUT <= 0:
        errors.append("ONEFS_API_TIMEOUT must be a positive integer")
    if not (1 <= LISTEN_PORT <= 65535):
        errors.append("LISTEN_PORT must be between 1 and 65535")
    if ALL_STATS_ENABLED:
        if ALL_POLL_INTERVAL <= 0:
            errors.append("ALL_POLL_INTERVAL_SECONDS must be a positive integer")
        if ALL_BATCH_SIZE <= 0:
            errors.append("ALL_BATCH_SIZE must be a positive integer")
    if LOG_LEVEL.upper() not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        errors.append(
            "LOG_LEVEL must be one of DEBUG/INFO/WARNING/ERROR/CRITICAL"
        )

    if errors:
        logger.critical("invalid configuration:")
        for e in errors:
            logger.critical("  - %s", e)
        sys.exit(1)


def preflight_check():
    logger.info("checking connectivity to %s as '%s' ...", ENDPOINT, USERNAME)
    try:
        onefs_get("/platform/1/cluster/config")
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            logger.critical(
                "FATAL: authentication failed (HTTP %s) for user '%s' at %s "
                "— check ONEFS_USERNAME/ONEFS_PASSWORD",
                e.code, USERNAME, ENDPOINT,
            )
        else:
            logger.critical(
                "FATAL: OneFS API at %s returned HTTP %s: %s", ENDPOINT, e.code, e
            )
        sys.exit(1)
    except urllib.error.URLError as e:
        logger.critical("FATAL: cannot reach OneFS API at %s: %s", ENDPOINT, e)
        sys.exit(1)
    logger.info("connectivity OK")


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
            logger.warning("scrape failed: %s", e)
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
            logger.info(
                "full-catalog sweep done in %.1fs, %d lines",
                dur, text.count(chr(10)),
            )
        except Exception as e:
            with _all_lock:
                _all_last_error = str(e)
            logger.warning("full-catalog sweep failed: %s", e)
        time.sleep(max(ALL_POLL_INTERVAL - (time.time() - start), 5))


def _fmt_ts(ts):
    if not ts:
        return "never"
    age = time.time() - ts
    return f"{time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(ts))} ({int(age)}s ago)"


def render_index_html():
    with _lock:
        last_success = _last_success
        err = _last_error
    with _all_lock:
        all_err = _all_last_error
        all_dur = _all_last_duration

    status = "OK" if not err else "ERROR"
    status_color = "#2e7d32" if not err else "#c62828"
    all_status = "disabled" if not ALL_STATS_ENABLED else ("OK" if not all_err else "ERROR")

    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>onefs-exporter</title>
<style>
  body {{ font-family: -apple-system, Helvetica, Arial, sans-serif; max-width: 720px; margin: 40px auto; color: #222; line-height: 1.5; }}
  h1 {{ font-size: 1.4em; }}
  table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
  td, th {{ text-align: left; padding: 6px 10px; border-bottom: 1px solid #ddd; font-size: 0.92em; }}
  th {{ color: #666; width: 40%; }}
  a.metrics-link {{ display: inline-block; margin-top: 0.5em; padding: 8px 16px; background: #2e7d32; color: white; text-decoration: none; border-radius: 4px; }}
  code {{ background: #f2f2f2; padding: 1px 5px; border-radius: 3px; }}
  .status {{ font-weight: bold; }}
  footer {{ margin-top: 2em; font-size: 0.8em; color: #888; }}
</style>
</head>
<body>
  <h1>onefs-exporter</h1>
  <p>Prometheus exporter for Dell PowerScale (OneFS) — <code>{ENDPOINT}</code></p>
  <table>
    <tr><th>Curated metrics</th><td class="status" style="color:{status_color}">{status}</td></tr>
    <tr><th>Last successful poll</th><td>{_fmt_ts(last_success)}</td></tr>
    <tr><th>Poll interval</th><td>{POLL_INTERVAL}s</td></tr>
    <tr><th>Full-catalog metrics</th><td>{all_status}</td></tr>
    <tr><th>Last full-catalog sweep</th><td>{all_dur:.1f}s duration</td></tr>
    <tr><th>Full-catalog poll interval</th><td>{ALL_POLL_INTERVAL}s</td></tr>
  </table>
  <a class="metrics-link" href="/metrics">View /metrics</a>
  <p style="margin-top:1em; font-size:0.85em; color:#666;">
    Health checks: <a href="/healthz"><code>/healthz</code></a> (liveness) &middot;
    <a href="/readyz"><code>/readyz</code></a> (readiness)
  </p>
  <footer>
    <a href="https://github.com/azwellai/onefs-exporter">azwellai/onefs-exporter</a>
  </footer>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path in ("/", "/index.html"):
            data = render_index_html().encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path.startswith("/metrics"):
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
        elif self.path.startswith("/healthz"):
            data = b"ok\n"
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        elif self.path.startswith("/readyz"):
            with _lock:
                last_success = _last_success
                err = _last_error
            now = time.time()
            stale_threshold = max(3 * POLL_INTERVAL, 90)
            age = int(now - last_success) if last_success > 0 else None
            ready = last_success > 0 and (now - last_success) <= stale_threshold
            payload = {
                "ready": ready,
                "last_success_unix": int(last_success),
                "age_seconds": age,
                "last_error": err,
                "stale_threshold_seconds": stale_threshold,
            }
            data = (json.dumps(payload) + "\n").encode()
            self.send_response(200 if ready else 503)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, fmt, *args):
        logger.debug(fmt, *args)


if __name__ == "__main__":
    validate_config()
    preflight_check()
    threading.Thread(target=poll_loop, daemon=True).start()
    threading.Thread(target=poll_loop_all, daemon=True).start()
    logger.info(
        "listening on :%d, polling %s every %ds (curated) / %ds (full catalog)",
        LISTEN_PORT, ENDPOINT, POLL_INTERVAL, ALL_POLL_INTERVAL,
    )
    ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler).serve_forever()
