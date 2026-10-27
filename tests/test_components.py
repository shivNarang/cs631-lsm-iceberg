import os
import random
import tempfile
import unittest

from lsm_iceberg.bloom import BloomFilter
from lsm_iceberg.memtable import DELETE, PUT, MemTable
from lsm_iceberg.puffin import Blob, read_puffin, write_puffin
from lsm_iceberg.wal import WriteAheadLog


class MemTableTest(unittest.TestCase):
    def test_sorted_and_overwrite(self):
        mt = MemTable(seed=1)
        keys = [f"k{i:05d}" for i in range(2000)]
        random.Random(2).shuffle(keys)
        for s, k in enumerate(keys, 1):
            mt.put(k, s, PUT, "v" + k)
        mt.put("k00010", 9999, DELETE, None)
        self.assertEqual(len(mt), 2000)
        out = [k for k, *_ in mt.items()]
        self.assertEqual(out, sorted(keys))
        self.assertEqual(mt.get("k00010"), (9999, DELETE, None))
        self.assertIsNone(mt.get("nope"))
        self.assertEqual([k for k, *_ in mt.items("k00100", "k00103")],
                         ["k00100", "k00101", "k00102", "k00103"])


class BloomTest(unittest.TestCase):
    def test_no_false_negatives_and_fpr(self):
        bf = BloomFilter.for_capacity(10_000, 0.01)
        for i in range(10_000):
            bf.add(f"in{i}")
        self.assertTrue(all(bf.might_contain(f"in{i}") for i in range(10_000)))
        fp = sum(bf.might_contain(f"out{i}") for i in range(10_000)) / 10_000
        self.assertLess(fp, 0.03)
        bf2 = BloomFilter.from_bytes(bf.to_bytes())
        self.assertTrue(bf2.might_contain("in42"))


class PuffinTest(unittest.TestCase):
    def test_roundtrip(self):
        path = os.path.join(tempfile.mkdtemp(), "x.puffin")
        size, footer, meta = write_puffin(path, [Blob("a", [1], b"hello"), Blob("b", [1, 2], b"\x00\x01")])
        self.assertEqual(size, os.path.getsize(path))
        blobs = read_puffin(path)
        self.assertEqual(blobs["a"][1], b"hello")
        self.assertEqual(blobs["b"][1], b"\x00\x01")
        with open(path, "rb") as f:
            self.assertEqual(f.read(4), b"PFA1")


class WALTest(unittest.TestCase):
    def test_replay_tolerates_torn_tail(self):
        path = os.path.join(tempfile.mkdtemp(), "w", "x.wal")
        w = WriteAheadLog(path)
        w.append(1, PUT, "a", "1")
        w.append(2, DELETE, "a", None)
        w.close()
        with open(path, "ab") as f:
            f.write(b'[3,0,"b"')  # crash mid-write
        self.assertEqual(list(WriteAheadLog.replay(path)), [(1, PUT, "a", "1"), (2, DELETE, "a", None)])


if __name__ == "__main__":
    unittest.main()
