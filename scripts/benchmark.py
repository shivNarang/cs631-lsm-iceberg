"""Preliminary benchmark: LSM-on-Iceberg vs. vanilla Iceberg (append micro-batches).

YCSB-style workload: load N keys, then a stream of updates/deletes with a skewed
key choice, then point lookups (hits + misses) and short range scans.
Both tables flush every --batch writes, so both end up with the same number of files.

python scripts/benchmark.py [--keys 20000 --updates 40000 --batch 5000 --lookups 200]
"""
import argparse
import json
import os
import random
import shutil
import statistics
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
from lsm_iceberg import LSMConfig, LSMTable, VanillaIcebergTable, get_catalog  # noqa: E402


def pct(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def workload(keys, updates, seed):
    rng = random.Random(seed)
    order = list(range(keys))
    rng.shuffle(order)  # streaming arrivals are not key-ordered
    for i in order:
        yield "put", f"user{i:08d}", f"load-{i}"
    for j in range(updates):
        k = int(keys * (rng.random() ** 2))  # skew: low ids are hot
        if rng.random() < 0.1:
            yield "delete", f"user{k:08d}", None
        else:
            yield "put", f"user{k:08d}", f"upd-{j}"


def dir_bytes(path):
    return sum(os.path.getsize(os.path.join(d, f)) for d, _, fs in os.walk(path) for f in fs)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--keys", type=int, default=20_000)
    ap.add_argument("--updates", type=int, default=40_000)
    ap.add_argument("--batch", type=int, default=5_000)
    ap.add_argument("--lookups", type=int, default=200)
    ap.add_argument("--scans", type=int, default=20)
    ap.add_argument("--scan-width", type=int, default=100)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--cache-blocks", type=int, default=256, help="0 = no block cache (every block read from disk)")
    a = ap.parse_args()

    wh = os.path.join(ROOT, "warehouse", "bench")
    shutil.rmtree(wh, ignore_errors=True)
    cat = get_catalog(wh)
    lsm = LSMTable(cat, "bench.lsm_kv", LSMConfig(memtable_max_entries=a.batch, row_group_size=1024, block_cache_blocks=a.cache_blocks))
    van = VanillaIcebergTable(cat, "bench.vanilla_kv", batch_size=a.batch)
    total_ops = a.keys + a.updates
    res = {"config": vars(a)}

    # ---- writes
    for name, t in (("lsm", lsm), ("vanilla", van)):
        t0 = time.perf_counter()
        for op, k, v in workload(a.keys, a.updates, a.seed):
            t.put(k, v) if op == "put" else t.delete(k)
        t.flush()
        dt = time.perf_counter() - t0
        res[name] = {"write_s": round(dt, 2), "write_ops_per_s": round(total_ops / dt)}
    res["lsm"]["data_files"] = sum(len(r) for r in lsm.levels.values())
    res["vanilla"]["data_files"] = len(van.table.inspect.files())
    res["lsm"]["table_bytes"] = dir_bytes(os.path.join(wh, "bench", "lsm_kv"))
    res["vanilla"]["table_bytes"] = dir_bytes(os.path.join(wh, "bench", "vanilla_kv"))

    # ---- point lookups: 80% existing keys (hot-skewed), 20% never-written keys
    rng = random.Random(a.seed + 1)
    keys = [f"user{int(a.keys * rng.random() ** 2):08d}" if rng.random() < 0.8 else f"user{rng.randrange(a.keys):08d}x"
            for _ in range(a.lookups)]
    mismatches = 0
    lsm.stats.reset()
    for name, t in (("lsm", lsm), ("vanilla", van)):
        lat, files = [], []
        answers = []
        for k in keys:
            t0 = time.perf_counter()
            answers.append(t.get(k))
            lat.append((time.perf_counter() - t0) * 1000)
            if name == "vanilla":
                files.append(t.files_scanned_last)
        res[name].update({"get_p50_ms": round(pct(lat, 50), 3), "get_p95_ms": round(pct(lat, 95), 3),
                          "get_mean_ms": round(statistics.mean(lat), 3)})
        if name == "lsm":
            lsm_answers = answers
            s = lsm.stats
            res["lsm"].update({
                "files_read_per_get": round(s.blocks_read / a.lookups + s.block_cache_hits / a.lookups, 2),
                "runs_skipped_by_range_or_bloom_pct": round(100 * (s.range_pruned + s.bloom_pruned) / max(1, s.runs_checked), 1),
                "bloom_false_positives": s.bloom_false_positives})
        else:
            res["vanilla"]["files_read_per_get"] = round(statistics.mean(files), 2)
            mismatches = sum(x != y for x, y in zip(lsm_answers, answers))

    # ---- range scans
    for name, t in (("lsm", lsm), ("vanilla", van)):
        rng = random.Random(a.seed + 2)
        lat = []
        for _ in range(a.scans):
            lo = rng.randrange(a.keys - a.scan_width)
            t0 = time.perf_counter()
            list(t.scan(f"user{lo:08d}", f"user{lo + a.scan_width:08d}"))
            lat.append((time.perf_counter() - t0) * 1000)
        res[name]["scan_p50_ms"] = round(pct(lat, 50), 2)

    res["answers_disagree"] = mismatches
    lsm.close()

    rows = ["write_ops_per_s", "data_files", "table_bytes", "get_p50_ms", "get_p95_ms",
            "files_read_per_get", "scan_p50_ms"]
    print(f"\nWorkload: {a.keys:,} keys loaded + {a.updates:,} updates/deletes, flush every {a.batch:,} writes")
    print(f"{'metric':32s}{'LSM-Iceberg':>16s}{'Vanilla Iceberg':>18s}")
    for r in rows:
        print(f"{r:32s}{str(res['lsm'].get(r, '-')):>16s}{str(res['vanilla'].get(r, '-')):>18s}")
    print(f"{'runs skipped (range/bloom) %':32s}{res['lsm']['runs_skipped_by_range_or_bloom_pct']:>16}")
    print(f"answers that differ between the two tables: {mismatches} / {a.lookups}")
    os.makedirs(os.path.join(ROOT, "results"), exist_ok=True)
    out = "benchmark.json" if a.cache_blocks else "benchmark_nocache.json"
    with open(os.path.join(ROOT, "results", out), "w") as f:
        json.dump(res, f, indent=2)
    print(f"saved results/{out}")


if __name__ == "__main__":
    main()
