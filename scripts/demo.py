"""Checkpoint demo: walk through the write path, read path and crash recovery.

python scripts/demo.py            (uses ./warehouse, table demo.kv, starts fresh)
"""
import os
import shutil
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from lsm_iceberg import LSMConfig, LSMTable, get_catalog  # noqa: E402

WAREHOUSE = os.path.join(ROOT, "warehouse", "demo")


def step(title):
    print("\n" + "=" * 72 + f"\n{title}\n" + "=" * 72)


def show_get(t, key):
    trace = []
    v = t.get(key, trace)
    print(f"get({key!r}) = {v!r}")
    for line in trace:
        print(f"    {line}")


def main():
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    cfg = LSMConfig(memtable_max_entries=1000, row_group_size=128)
    t = LSMTable(get_catalog(WAREHOUSE), "demo.kv", cfg)

    step("1. WRITE PATH: 2,500 puts -> WAL -> MemTable; flushes every 1,000 keys")
    for i in range(2500):
        t.put(f"user{(i * 37) % 2500:05d}", f"profile-v1-{i}")
    for r in sorted((r for runs in t.levels.values() for r in runs), key=lambda r: r.meta.run_id):
        m = r.meta
        print(f"  run {m.run_id}: {m.record_count} rows, keys [{m.min_key} .. {m.max_key}], "
              f"seq [{m.min_seq}..{m.max_seq}], {m.row_groups} row groups, {m.file_size} bytes")
    print(f"  MemTable still holds {len(t.memtable)} un-flushed entries (protected by the WAL)")
    print(f"  Iceberg snapshots: {len(t.table.metadata.snapshots)}, "
          f"Puffin index files: {len(t.table.metadata.statistics)}")

    step("2. UPDATES + DELETES (newer versions shadow older ones)")
    t.put("user00042", "profile-v2-UPDATED")
    t.delete("user00007")
    t.flush()
    t.put("user00099", "profile-v3-in-memtable")
    print("  put user00042 (flushed), delete user00007 (flushed tombstone), put user00099 (memtable only)")

    step("3. READ PATH: point lookups with a trace of every decision")
    show_get(t, "user00099")   # memtable hit
    show_get(t, "user00042")   # newest run
    show_get(t, "user00007")   # tombstone
    show_get(t, "user01234")   # older run, bloom filters skip others
    show_get(t, "user99999")   # not present: pruned by key range
    show_get(t, "user00042a")  # within ranges, bloom says no

    step("4. RANGE SCAN: k-way merge over MemTable + all runs")
    for k, v in t.scan("user00040", "user00045"):
        print(f"  {k} -> {v}")

    step("5. CRASH RECOVERY: write 3 keys, 'crash' without flushing, reopen")
    for k in ("crash-a", "crash-b", "crash-c"):
        t.put(k, "survives")
    del t  # no flush(), no close()
    t = LSMTable(get_catalog(WAREHOUSE), "demo.kv", cfg)
    print(f"  replayed {t.recovered} WAL records; get('crash-b') = {t.get('crash-b')!r}")

    step("6. STATS")
    for k, v in t.describe().items():
        print(f"  {k}: {v}")
    t.close()
    print(f"\nInspect the Iceberg metadata tree with:\n  python scripts/inspect_metadata.py demo.kv --warehouse {WAREHOUSE}")


if __name__ == "__main__":
    main()
