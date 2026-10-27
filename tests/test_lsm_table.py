import os
import random
import tempfile
import unittest

from lsm_iceberg import LSMConfig, LSMTable, get_catalog
from lsm_iceberg.memtable import PUT


def iceberg_resolved_view(table):
    """What any Iceberg engine sees after resolving 'latest seq wins'."""
    latest = {}
    for r in table.scan().to_arrow().to_pylist():
        cur = latest.get(r["key"])
        if cur is None or r["seq"] > cur["seq"]:
            latest[r["key"]] = r
    return {k: r["value"] for k, r in latest.items() if r["op"] == PUT}


class LSMTableTest(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="lsm test ")
        self.cfg = LSMConfig(memtable_max_entries=300, row_group_size=32)

    def open(self):
        return LSMTable(get_catalog(self.dir), "t.kv", self.cfg)

    def random_workload(self, t, n=3000, keyspace=800, seed=7):
        rng = random.Random(seed)
        model = {}
        for i in range(n):
            k = f"user{rng.randrange(keyspace):06d}"
            if rng.random() < 0.15:
                t.delete(k)
                model.pop(k, None)
            else:
                t.put(k, f"v{i}")
                model[k] = f"v{i}"
        return model

    def test_matches_model_across_many_flushes(self):
        t = self.open()
        model = self.random_workload(t)
        self.assertGreaterEqual(len(t.levels[0]), 5)
        for i in range(800):
            k = f"user{i:06d}"
            self.assertEqual(t.get(k), model.get(k), k)
        self.assertEqual(dict(t.scan()), model)
        self.assertEqual(list(t.scan("user000100", "user000200")),
                         sorted((k, v) for k, v in model.items() if "user000100" <= k <= "user000200"))
        t.close()

    def test_each_flush_is_an_iceberg_snapshot_with_puffin_index(self):
        t = self.open()
        self.random_workload(t, n=1500)
        runs = len(t.levels[0])
        snaps = t.table.metadata.snapshots
        self.assertEqual(len(snaps), runs)
        self.assertEqual(snaps[-1].summary.additional_properties["lsm.operation"], "flush")
        self.assertEqual(len(t.table.metadata.statistics), runs)
        for s in t.table.metadata.statistics:
            self.assertTrue(os.path.exists(s.statistics_path))
        t.close()

    def test_iceberg_readers_see_same_data(self):
        t = self.open()
        model = self.random_workload(t)
        t.flush()
        self.assertEqual(iceberg_resolved_view(t.table), model)
        t.close()

    def test_crash_recovery_from_wal(self):
        t = self.open()
        model = self.random_workload(t, n=1000)
        unflushed = len(t.memtable)
        self.assertGreater(unflushed, 0)
        # simulate crash: no flush, no close()
        t2 = self.open()
        self.assertGreaterEqual(t2.recovered, unflushed)  # log records >= distinct keys
        self.assertEqual(len(t2.memtable), unflushed)
        self.assertEqual(dict(t2.scan()), model)
        t2.put("after", "restart")  # sequence numbers continue correctly
        self.assertEqual(t2.get("after"), "restart")
        t2.close()

    def test_missing_index_is_rebuilt(self):
        t = self.open()
        model = self.random_workload(t, n=1000)
        t.flush()
        t.close()
        os.remove(t.levels[0][0].meta.index_file)
        t2 = self.open()
        self.assertEqual(dict(t2.scan()), model)
        self.assertTrue(os.path.exists(t.levels[0][0].meta.index_file))
        t2.close()

    def test_bloom_filters_prune_runs(self):
        t = self.open()
        for i in range(3000):
            t.put(f"key{(i * 7919) % 100000:06d}", "x")
        t.flush()
        t.stats.reset()
        for i in range(500):
            t.get(f"absent{i}")  # outside every run's key range
            t.get(f"key{i * 2 + 1:06d}x")  # inside ranges, never written
        s = t.stats
        self.assertGreater(s.range_pruned + s.bloom_pruned, 0.9 * s.runs_checked)
        t.close()


if __name__ == "__main__":
    unittest.main()
