# onefs_exporter의 순수/파싱 로직을 검증하는 stdlib unittest 스위트
import os
import tempfile
import threading
import unittest
from unittest import mock

import onefs_exporter as ox


class SanitizeMetricNameTest(unittest.TestCase):
    def test_dots_become_underscores_with_prefix(self):
        self.assertEqual(
            ox.sanitize_metric_name("cluster.disk.xfers.in.rate"),
            "onefs_raw_cluster_disk_xfers_in_rate",
        )

    def test_hostile_key_special_chars(self):
        self.assertEqual(
            ox.sanitize_metric_name('a b"c\\d.e'),
            "onefs_raw_a_b_c_d_e",
        )

    def test_alnum_and_underscore_preserved(self):
        self.assertEqual(
            ox.sanitize_metric_name("Node_1abc"),
            "onefs_raw_Node_1abc",
        )


class ChunkedTest(unittest.TestCase):
    def test_exact_batching(self):
        self.assertEqual(
            list(ox.chunked([1, 2, 3, 4, 5], 2)),
            [[1, 2], [3, 4], [5]],
        )

    def test_empty_list_no_batches(self):
        self.assertEqual(list(ox.chunked([], 3)), [])

    def test_size_larger_than_list(self):
        self.assertEqual(list(ox.chunked([1, 2], 10)), [[1, 2]])


class EscTest(unittest.TestCase):
    def test_escapes_backslash_and_quote(self):
        self.assertEqual(ox.esc('a\\b"c'), 'a\\\\b\\"c')

    def test_plain_value_unchanged(self):
        self.assertEqual(ox.esc("nfs"), "nfs")

    def test_non_string_coerced(self):
        self.assertEqual(ox.esc(42), "42")


class FetchStatsTest(unittest.TestCase):
    def test_groups_by_key_and_drops_errors(self):
        payload = {
            "stats": [
                {"key": "a", "devid": 1, "value": 10, "error": None},
                {"key": "a", "devid": 2, "value": 20, "error": None},
                {"key": "b", "devid": 1, "value": 5, "error": None},
                {"key": "c", "devid": 1, "value": None, "error": "boom"},
            ]
        }
        with mock.patch.object(ox, "onefs_get", return_value=payload) as m:
            result = ox.fetch_stats(["a", "b", "c"])
        self.assertEqual(len(result["a"]), 2)
        self.assertEqual(len(result["b"]), 1)
        self.assertNotIn("c", result)
        # nodes=all not requested by default
        _, kwargs = m.call_args
        params = m.call_args[0][1]
        self.assertNotIn("nodes", params)
        self.assertEqual(params["keys"], "a,b,c")

    def test_nodes_all_param_passed(self):
        with mock.patch.object(ox, "onefs_get", return_value={"stats": []}) as m:
            ox.fetch_stats(["x"], nodes_all=True)
        params = m.call_args[0][1]
        self.assertEqual(params["nodes"], "all")


class FetchCatalogTest(unittest.TestCase):
    def test_numeric_only_and_scope_split(self):
        payload = {
            "keys": [
                {"key": "cluster.a", "type": "uint64", "scope": "cluster"},
                {"key": "cluster.b", "type": "double", "scope": "cluster"},
                {"key": "node.a", "type": "int32", "scope": "node"},
                {"key": "node.b", "type": "int64", "scope": "node"},
                {"key": "skip.string", "type": "string", "scope": "cluster"},
                {"key": "skip.scope", "type": "uint64", "scope": "drive"},
            ]
        }
        with mock.patch.object(ox, "onefs_get", return_value=payload):
            cluster_keys, node_keys = ox.fetch_catalog()
        self.assertEqual(cluster_keys, ["cluster.a", "cluster.b"])
        self.assertEqual(node_keys, ["node.a", "node.b"])


def _collect_fetch_stats(keys, nodes_all=False):
    if nodes_all:
        return {
            "node.health": [
                {"devid": 1, "value": 0},
                {"devid": 2, "value": 1},
            ],
            "node.cpu.idle.avg": [{"devid": 1, "value": 989}],
            "node.memory.used": [{"devid": 2, "value": 4096}],
        }
    return {
        "ifs.bytes.total": [{"value": 1000}],
        "ifs.bytes.avail": [{"value": 400}],
        "cluster.health": [{"value": 0}],
        "cluster.alert.info": [{"value": ["x", "y", "z"]}],
        "cluster.cpu.sys.avg": [{"value": 50}],
        "cluster.protostats.nfs.total": [
            {"value": [{"op_rate": 5, "in_rate": 10, "out_rate": 20}]}
        ],
        "cluster.protostats.smb2.total": [{"value": []}],
    }


class CollectTest(unittest.TestCase):
    def setUp(self):
        self.collector = ox.CuratedCollector()
        self.p_stats = mock.patch.object(
            ox, "fetch_stats", side_effect=_collect_fetch_stats
        )
        self.p_jobs = mock.patch.object(
            ox, "fetch_running_jobs", return_value=[{"id": 1}, {"id": 2}]
        )
        self.p_stats.start()
        self.p_jobs.start()

    def tearDown(self):
        self.p_stats.stop()
        self.p_jobs.stop()

    def test_cluster_and_node_metrics(self):
        out = self.collector.collect()
        self.assertIn("onefs_cluster_capacity_total_bytes 1000", out)
        self.assertIn("onefs_cluster_health 0", out)
        # alert.info emits len() of the list
        self.assertIn("onefs_cluster_alert_count 3", out)
        # cpu.sys scaled /10
        self.assertIn("onefs_cluster_cpu_sys_percent 5.0", out)
        # node cpu idle scaled *0.1
        self.assertIn(f'onefs_node_cpu_idle_percent{{node="1"}} {989 * 0.1}', out)
        self.assertIn('onefs_node_health{node="1"} 0', out)
        self.assertIn('onefs_node_health{node="2"} 1', out)

    def test_protocol_lines_only_for_nonempty(self):
        out = self.collector.collect()
        self.assertIn('onefs_protocol_op_rate{protocol="nfs"} 5', out)
        self.assertIn('onefs_protocol_in_rate_bytes{protocol="nfs"} 10', out)
        self.assertIn('onefs_protocol_out_rate_bytes{protocol="nfs"} 20', out)
        # smb2 had an empty value list -> no sample lines
        self.assertNotIn('protocol="smb2"', out)
        # http/nfs4 had no data at all
        self.assertNotIn('protocol="http"', out)

    def test_running_jobs_and_scrape_success(self):
        out = self.collector.collect()
        self.assertIn("onefs_job_engine_running_jobs 2", out)
        self.assertIn("onefs_exporter_scrape_success 1", out)


class CollectAllTest(unittest.TestCase):
    def setUp(self):
        self.collector = ox.FullCatalogCollector()
        self.collector.catalog_cluster_keys = ["cluster.k1"]
        self.collector.catalog_node_keys = ["node.k1"]

    @staticmethod
    def _fake_onefs_get(path, params=None):
        # fetch_stats builds params; distinguish node vs cluster by 'nodes'
        if params and params.get("nodes") == "all":
            return {
                "stats": [
                    {"key": "node.k1", "devid": 0, "value": 11, "error": None},
                    {"key": "node.k1", "devid": 2, "value": 22, "error": None},
                    {"key": "node.k1", "devid": 3, "value": "nope", "error": None},
                ]
            }
        return {
            "stats": [
                {"key": "cluster.k1", "devid": None, "value": 7, "error": None},
                {"key": "cluster.k1", "devid": None, "value": [1, 2], "error": None},
            ]
        }

    def test_numeric_emission_and_labels(self):
        with mock.patch.object(ox, "onefs_get", side_effect=self._fake_onefs_get):
            out = self.collector.collect_all()
        # cluster value (devid passed as None): no node label
        self.assertIn("onefs_raw_cluster_k1 7", out)
        self.assertNotIn('onefs_raw_cluster_k1{node=', out)
        # devid=0 -> must emit node="0" label (not collide with cluster scope)
        self.assertIn('onefs_raw_node_k1{node="0"} 11', out)
        # and must NOT emit an unlabelled node sample
        self.assertNotIn("onefs_raw_node_k1 11", out)
        # devid=2 -> labelled
        self.assertIn('onefs_raw_node_k1{node="2"} 22', out)
        # non-numeric values (list / string) skipped
        self.assertNotIn("[1, 2]", out)
        self.assertNotIn("nope", out)

    def test_help_type_emitted_once_per_metric(self):
        with mock.patch.object(ox, "onefs_get", side_effect=self._fake_onefs_get):
            out = self.collector.collect_all()
        self.assertEqual(out.count("# HELP onefs_raw_node_k1 "), 1)
        self.assertEqual(out.count("# TYPE onefs_raw_node_k1 gauge"), 1)
        self.assertEqual(out.count("# HELP onefs_raw_cluster_k1 "), 1)

    def test_catalog_loaded_when_empty(self):
        self.collector.catalog_cluster_keys = []
        self.collector.catalog_node_keys = []
        catalog = (["cluster.k1"], ["node.k1"])
        with mock.patch.object(ox, "fetch_catalog", return_value=catalog) as fc, \
                mock.patch.object(ox, "onefs_get", side_effect=self._fake_onefs_get):
            self.collector.collect_all()
        fc.assert_called_once()
        self.assertEqual(self.collector.catalog_cluster_keys, ["cluster.k1"])

    def test_instances_do_not_share_catalog_state(self):
        # Each FullCatalogCollector owns its own catalog; mutating one (as
        # collect_all does on first sweep) must not leak into another.
        a = ox.FullCatalogCollector()
        b = ox.FullCatalogCollector()
        a.catalog_cluster_keys.append("cluster.only_a")
        a.catalog_node_keys.append("node.only_a")
        self.assertEqual(b.catalog_cluster_keys, [])
        self.assertEqual(b.catalog_node_keys, [])
        self.assertIsNot(a.catalog_cluster_keys, b.catalog_cluster_keys)


_VALID = dict(
    ENDPOINT="host:8080",
    USERNAME="user",
    PASSWORD="pw",
    _password_file_error="",
    POLL_INTERVAL=30,
    TIMEOUT=10,
    LISTEN_PORT=9684,
    ALL_STATS_ENABLED=True,
    ALL_POLL_INTERVAL=300,
    ALL_BATCH_SIZE=200,
    LOG_LEVEL="INFO",
)


class ResolvePasswordTest(unittest.TestCase):
    def _write(self, content):
        f = tempfile.NamedTemporaryFile("w", delete=False)
        f.write(content)
        f.close()
        self.addCleanup(os.unlink, f.name)
        return f.name

    def test_file_wins_over_env_password(self):
        path = self._write("filepw")
        pw, err = ox._resolve_password("envpw", path)
        self.assertEqual(pw, "filepw")
        self.assertEqual(err, "")

    def test_trailing_newline_stripped(self):
        path = self._write("s3cret\n")
        pw, err = ox._resolve_password("", path)
        self.assertEqual(pw, "s3cret")
        self.assertEqual(err, "")

    def test_preserves_non_newline_whitespace(self):
        # leading/trailing spaces are legitimate password chars; keep them
        path = self._write("  pass word  \n")
        pw, err = ox._resolve_password("", path)
        self.assertEqual(pw, "  pass word  ")
        self.assertEqual(err, "")

    def test_no_file_falls_back_to_env(self):
        pw, err = ox._resolve_password("envpw", "")
        self.assertEqual(pw, "envpw")
        self.assertEqual(err, "")

    def test_missing_file_returns_error(self):
        pw, err = ox._resolve_password("envpw", "/no/such/onefs/pw/file")
        self.assertEqual(pw, "")
        self.assertIn("/no/such/onefs/pw/file", err)

    def test_empty_file_returns_error(self):
        path = self._write("\n")
        pw, err = ox._resolve_password("envpw", path)
        self.assertEqual(pw, "")
        self.assertIn(path, err)
        self.assertIn("empty", err)


class ValidateConfigTest(unittest.TestCase):
    def test_valid_config_passes(self):
        with mock.patch.multiple(ox, **_VALID):
            ox.validate_config()  # should not raise

    def test_missing_username_exits(self):
        cfg = dict(_VALID, USERNAME="")
        with mock.patch.multiple(ox, **cfg):
            with self.assertRaises(SystemExit) as cm:
                ox.validate_config()
        self.assertEqual(cm.exception.code, 1)

    def test_bad_port_exits(self):
        cfg = dict(_VALID, LISTEN_PORT=70000)
        with mock.patch.multiple(ox, **cfg):
            with self.assertRaises(SystemExit):
                ox.validate_config()

    def test_bad_log_level_exits(self):
        cfg = dict(_VALID, LOG_LEVEL="VERBOSE")
        with mock.patch.multiple(ox, **cfg):
            with self.assertRaises(SystemExit):
                ox.validate_config()

    def test_nonpositive_poll_interval_exits(self):
        cfg = dict(_VALID, POLL_INTERVAL=0)
        with mock.patch.multiple(ox, **cfg):
            with self.assertRaises(SystemExit):
                ox.validate_config()

    def test_no_password_at_all_exits_with_combined_message(self):
        cfg = dict(_VALID, PASSWORD="", _password_file_error="")
        with mock.patch.multiple(ox, **cfg):
            with self.assertLogs(ox.logger, level="CRITICAL") as log:
                with self.assertRaises(SystemExit) as cm:
                    ox.validate_config()
        self.assertEqual(cm.exception.code, 1)
        self.assertTrue(
            any("ONEFS_PASSWORD or ONEFS_PASSWORD_FILE" in m for m in log.output)
        )

    def test_password_file_unreadable_exits_with_specific_error(self):
        _, err = ox._resolve_password("", "/no/such/onefs/pw/file")
        cfg = dict(_VALID, PASSWORD="", _password_file_error=err)
        with mock.patch.multiple(ox, **cfg):
            with self.assertLogs(ox.logger, level="CRITICAL") as log:
                with self.assertRaises(SystemExit) as cm:
                    ox.validate_config()
        self.assertEqual(cm.exception.code, 1)
        self.assertTrue(any("/no/such/onefs/pw/file" in m for m in log.output))


class RenderIndexHtmlTest(unittest.TestCase):
    def test_contains_endpoint_and_metrics_link(self):
        with mock.patch.object(ox, "ENDPOINT", "my.onefs.local:8080"):
            html = ox.render_index_html()
        self.assertIsInstance(html, str)
        self.assertIn("my.onefs.local:8080", html)
        self.assertIn('href="/metrics"', html)


class SnapshotTest(unittest.TestCase):
    def test_curated_snapshot_consistent_under_concurrent_writes(self):
        c = ox.CuratedCollector()
        stop = threading.Event()

        def churn():
            i = 0
            while not stop.is_set():
                with c.lock:
                    c.cache_text = f"# text {i}\n"
                    c.last_success = float(i)
                    c.last_error = "" if i % 2 else "boom"
                i += 1

        writer = threading.Thread(target=churn)
        writer.start()
        try:
            for _ in range(500):
                snap = c.snapshot()
                # snapshot() must return a 3-tuple with no exception, regardless
                # of the concurrent writer.
                self.assertEqual(len(snap), 3)
                text, last_success, err = snap
                self.assertIsInstance(text, str)
                self.assertIsInstance(err, str)
        finally:
            stop.set()
            writer.join()

    def test_full_catalog_snapshot_shape(self):
        f = ox.FullCatalogCollector()
        text, err, dur = f.snapshot()
        self.assertIsInstance(text, str)
        self.assertEqual(err, "")
        self.assertEqual(dur, 0.0)


if __name__ == "__main__":
    unittest.main()
