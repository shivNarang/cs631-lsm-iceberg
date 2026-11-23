#!/usr/bin/env python3
"""Build final_report.html (repo root) from results/evaluation.json, usecase.json and spark.json.

Usage: python3 report/make_report.py   (run from lsm-iceberg-java/)
Then print to PDF with headless Chrome (see README).
"""
import json
import math
import os
from html import escape

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
RES = os.path.join(ROOT, "results")


def load(name):
    p = os.path.join(RES, name)
    return json.load(open(p)) if os.path.exists(p) else None


ev = load("evaluation.json")
uc = load("usecase.json") or {}
sp = load("spark.json") or {}

LSM = ["lsm", "lsm-parquet", "lsm-fence", "lsm-nocompact"]
BASE = ["append", "mor", "cow"]
NAMES = {
    "lsm": "LSM (ours)",
    "lsm-parquet": "LSM, Parquet runs",
    "lsm-fence": "LSM, fence pointers",
    "lsm-nocompact": "LSM, no merges",
    "append": "Iceberg append",
    "mor": "Iceberg merge-on-read",
    "cow": "Iceberg copy-on-write",
}
BLUE, ORANGE = "#2a78d6", "#eb6834"
INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#8a8984", "#e4e3df"
stores = [s for s in LSM + BASE if s in ev]
cfg = {k: (int(v) if str(v).isdigit() else v) for k, v in ev["config"].items()}


def color(s):
    return BLUE if s in LSM else ORANGE


def fmt(x, unit=""):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "–"
    if isinstance(x, (int,)) or (isinstance(x, float) and x >= 100):
        return f"{x:,.0f}{unit}"
    if x >= 10:
        return f"{x:.1f}{unit}"
    if x >= 1:
        return f"{x:.2f}{unit}"
    return f"{x:.3f}{unit}"


def hbar(title, metric, unit, log=False, note="", get=None):
    """Horizontal bar chart, one bar per table; value labels at the bar ends."""
    get = get or (lambda s: ev[s][metric])
    vals = [(s, get(s)) for s in stores]
    w, left, right, bh, gap, top = 640, 170, 70, 16, 8, 26
    h = top + len(vals) * (bh + gap) + 22
    plot = w - left - right
    vmax = max(v for _, v in vals)
    if log:
        vmin = min(v for _, v in vals if v > 0)
        lo, hi = math.floor(math.log10(vmin)), math.ceil(math.log10(vmax))
        pos = lambda v: plot * (math.log10(max(v, 10 ** lo)) - lo) / (hi - lo)
        ticks = [10 ** k for k in range(lo, hi + 1)]
    else:
        step = 10 ** math.floor(math.log10(vmax))
        top_v = math.ceil(vmax / step) * step
        pos = lambda v: plot * v / top_v
        n = int(top_v / step)
        ticks = [step * i for i in range(0, n + 1, max(1, n // 5))]
    out = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="{escape(title)}">']
    out.append(f'<text x="0" y="14" class="ct">{escape(title)}</text>')
    y0, y1 = top - 4, top + len(vals) * (bh + gap) - gap + 4
    for t in ticks:
        x = left + pos(t)
        out.append(f'<line x1="{x:.1f}" y1="{y0}" x2="{x:.1f}" y2="{y1}" stroke="{GRID}" stroke-width="1"/>')
        lab = f"{t:g}" if t < 1000 else f"{t/1000:g}k"
        out.append(f'<text x="{x:.1f}" y="{y1 + 13}" class="tk" text-anchor="middle">{lab}</text>')
    for i, (s, v) in enumerate(vals):
        y = top + i * (bh + gap)
        bw = max(2, pos(v)) if v > 0 else 0
        out.append(f'<text x="{left - 8}" y="{y + bh - 4}" class="lb" text-anchor="end">{escape(NAMES[s])}</text>')
        out.append(f'<rect x="{left}" y="{y}" width="{bw:.1f}" height="{bh}" rx="3" fill="{color(s)}"/>')
        out.append(f'<text x="{left + bw + 6:.1f}" y="{y + bh - 4}" class="vl">{fmt(v, unit)}</text>')
    out.append("</svg>")
    cap = f'<div class="cap">{note}</div>' if note else ""
    return "<figure>" + "".join(out) + cap + "</figure>"


def curve_chart():
    """Mean lookup latency as writes pile up: lsm vs append vs mor (log y)."""
    series = [s for s in ["lsm", "append", "mor"] if s in ev]
    cols = {"lsm": BLUE, "append": ORANGE, "mor": "#1baf7a"}
    w, h, l, r, t, b = 640, 230, 56, 190, 26, 34
    pw, ph = w - l - r, h - t - b
    xmax = max(p["writes"] for s in series for p in ev[s]["curve"])
    ys = [p["get_mean_ms"] for s in series for p in ev[s]["curve"] if p["get_mean_ms"] > 0]
    lo, hi = math.floor(math.log10(min(ys))), math.ceil(math.log10(max(ys)))
    X = lambda x: l + pw * x / xmax
    Y = lambda y: t + ph - ph * (math.log10(max(y, 10 ** lo)) - lo) / (hi - lo)
    out = [f'<svg viewBox="0 0 {w} {h}" width="100%" role="img" aria-label="lookup latency while loading">']
    out.append('<text x="0" y="14" class="ct">Mean lookup time while the table is loaded (ms, log scale)</text>')
    for k in range(lo, hi + 1):
        y = Y(10 ** k)
        out.append(f'<line x1="{l}" y1="{y:.1f}" x2="{l + pw}" y2="{y:.1f}" stroke="{GRID}"/>')
        out.append(f'<text x="{l - 6}" y="{y + 4:.1f}" class="tk" text-anchor="end">{10 ** k:g}</text>')
    for i in range(0, 6):
        x = xmax * i / 5
        out.append(f'<text x="{X(x):.1f}" y="{t + ph + 16}" class="tk" text-anchor="middle">{x/1000:.0f}k</text>')
    out.append(f'<text x="{l + pw / 2}" y="{h - 2}" class="tk" text-anchor="middle">writes so far</text>')
    for s in series:
        pts = [(X(p["writes"]), Y(p["get_mean_ms"])) for p in ev[s]["curve"]]
        d = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        out.append(f'<polyline points="{d}" fill="none" stroke="{cols[s]}" stroke-width="2" stroke-linejoin="round"/>')
        for x, y in pts:
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="3.5" fill="{cols[s]}" stroke="#fff" stroke-width="2"/>')
        last = ev[s]["curve"][-1]
        out.append(f'<text x="{pts[-1][0] + 8:.1f}" y="{pts[-1][1] + 4:.1f}" class="lb">{escape(NAMES[s])} '
                   f'({last["data_files"]} files)</text>')
    out.append("</svg>")
    return "<figure>" + "".join(out) + "</figure>"


def table(rows, head, cls="", numeric=True):
    h = "".join(f"<th>{escape(c)}</th>" for c in head)
    body = ""
    for r in rows:
        body += "<tr>" + "".join(f'<td class="{"n" if i and numeric else ""}">{c}</td>' for i, c in enumerate(r)) + "</tr>"
    return f'<table class="{cls}"><tr>{h}</tr>{body}</table>'


L = ev["lsm"]
best_iceberg_get = min(ev[s]["get_p50_ms"] for s in BASE if s in ev)
speed_vs_append = ev["append"]["get_p50_ms"] / L["get_p50_ms"]
speed_vs_cow = ev["cow"]["get_p50_ms"] / L["get_p50_ms"]

# ---- tables
main_rows = []
for s in stores:
    e = ev[s]
    main_rows.append([
        f'<span class="sw" style="background:{color(s)}"></span>{escape(NAMES[s])}',
        fmt(e["write_ops_per_s"]), fmt(e.get("row_write_amplification")), fmt(e["write_amplification"]), fmt(e["data_files"]),
        fmt(e["get_p50_ms"]), fmt(e["get_p95_ms"]), fmt(e["files_read_per_get"]), fmt(e["scan_p50_ms"]),
        escape(str(e["answers_differ"])),
    ])
ycsb_rows = []
for s in stores:
    y = ev[s]["ycsb_ops_per_s"]
    ycsb_rows.append([f'<span class="sw" style="background:{color(s)}"></span>{escape(NAMES[s])}'] + [fmt(y[w]) for w in "ABCE"])


def ratio(a, b):
    return a / b if b else float("nan")


abl = []
if "lsm-parquet" in ev:
    p = ev["lsm-parquet"]
    abl.append(["Avro runs instead of Parquet (professor's suggestion)",
                f'lookup {fmt(p["get_p50_ms"])} → {fmt(L["get_p50_ms"])} ms; YCSB-C {fmt(p["ycsb_ops_per_s"]["C"])} → {fmt(L["ycsb_ops_per_s"]["C"])} ops/s',
                f'files {ratio(L["table_bytes_live"], p["table_bytes_live"]):.1f}× bigger (no compression)'])
if "lsm-fence" in ev:
    f = ev["lsm-fence"]
    abl.append(["Key index in Puffin instead of fence pointers + binary search",
                f'YCSB-B {fmt(f["ycsb_ops_per_s"]["B"])} → {fmt(L["ycsb_ops_per_s"]["B"])}, YCSB-C {fmt(f["ycsb_ops_per_s"]["C"])} → {fmt(L["ycsb_ops_per_s"]["C"])} ops/s; '
                f'wasted block reads (bloom false positives) {f.get("bloom_false_positive_block_reads", 0)} → 0; median lookup the same within noise '
                f'({fmt(f["get_p50_ms"])} vs {fmt(L["get_p50_ms"])} ms), since one block read costs far more than a binary search',
                f'12 bytes per hash slot (about 17 bytes per key) in Puffin and in memory; all indexes of this table: {L["index_bytes"] / 1e6:.1f} MB'])
if "lsm-nocompact" in ev:
    n = ev["lsm-nocompact"]
    abl.append(["Leveled merges (compaction) vs none",
                f'files {n["data_files"]} → {L["data_files"]}; scan {fmt(n["scan_p50_ms"])} → {fmt(L["scan_p50_ms"])} ms; YCSB-E {fmt(n["ycsb_ops_per_s"]["E"])} → {fmt(L["ycsb_ops_per_s"]["E"])} ops/s',
                f'rows written per write {fmt(n.get("row_write_amplification"))} → {fmt(L.get("row_write_amplification"))}; writes {fmt(n["write_ops_per_s"])} → {fmt(L["write_ops_per_s"])} ops/s'])

ws = L.get("write_stats", {})

charts = (
    hbar("Point lookup, median (ms, log scale)", "get_p50_ms", " ms", log=True)
    + hbar("Files opened per lookup", "files_read_per_get", "",
           note="For the LSM tables this counts blocks read (one block = one file). Merge-on-read also opens delete files.")
    + hbar("Write throughput while loading (writes per second)", "write_ops_per_s", "")
)

HTML = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>LSM-Iceberg Final Report</title>
<style>
  @page {{ size: A4; margin: 14mm 16mm; }}
  :root {{ --ink:{INK}; --ink2:{INK2}; --muted:{MUTED}; --line:#cfcdc6; --blue:{BLUE}; --orange:{ORANGE}; }}
  body {{ font-family: "Helvetica Neue", Arial, sans-serif; font-size: 10pt; line-height: 1.45; color: var(--ink); background: #fff; margin: 0; }}
  h1 {{ font-size: 17pt; margin: 0 0 2px; }}
  h2 {{ font-size: 12pt; margin: 16px 0 5px; border-bottom: 1px solid var(--line); padding-bottom: 2px; break-after: avoid; }}
  h3 {{ font-size: 10.5pt; margin: 10px 0 3px; break-after: avoid; }}
  p {{ margin: 5px 0; }}
  .sub {{ color: var(--ink2); }}
  .team td {{ border: none; padding: 0 14px 0 0; }}
  table {{ border-collapse: collapse; width: 100%; margin: 6px 0; font-size: 9pt; break-inside: avoid; }}
  th, td {{ border-bottom: 1px solid var(--line); padding: 3px 6px; text-align: left; vertical-align: top; }}
  th {{ font-size: 8pt; text-transform: uppercase; letter-spacing: .04em; color: var(--ink2); }}
  td.n {{ text-align: right; font-variant-numeric: tabular-nums; }}
  table.wide td.n {{ white-space: nowrap; }}
  ul, ol {{ margin: 4px 0; padding-left: 18px; }} li {{ margin: 2px 0; }}
  code {{ font-family: Menlo, monospace; font-size: 8.4pt; }}
  pre {{ font-family: Menlo, monospace; font-size: 8pt; background: #f4f4f2; padding: 6px 8px; margin: 5px 0; white-space: pre-wrap; break-inside: avoid; }}
  figure {{ margin: 8px 0; break-inside: avoid; }}
  .ct {{ font-size: 11px; font-weight: 700; fill: var(--ink); }}
  .tk {{ font-size: 9.5px; fill: var(--muted); }}
  .lb {{ font-size: 10px; fill: var(--ink2); }}
  .vl {{ font-size: 10px; fill: var(--ink); font-variant-numeric: tabular-nums; }}
  .cap {{ font-size: 8.5pt; color: var(--ink2); }}
  .sw {{ display: inline-block; width: 9px; height: 9px; border-radius: 2px; margin-right: 5px; vertical-align: 0; }}
  .legend {{ font-size: 9pt; color: var(--ink2); margin: 4px 0; }}
  .key {{ background: #f1f5fb; border-left: 3px solid var(--blue); padding: 6px 10px; margin: 8px 0; break-inside: avoid; }}
  .two {{ display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }}
  .arch {{ font-family: Menlo, monospace; font-size: 8pt; line-height: 1.35; background: #f7f7f5; padding: 8px 10px; white-space: pre; break-inside: avoid; }}
</style></head><body>

<h1>LSM Tree Indexing in Apache Iceberg</h1>
<div class="sub">CS631 course project, final report</div>
<table class="team" style="width:auto"><tr><td>Shiv Narang</td><td>23b1075</td></tr>
<tr><td>Kaushal Vijayvergiya</td><td>23b0901</td></tr><tr><td>Kritin Gupta</td><td>23b0948</td></tr></table>

<div class="key"><b>Summary.</b> We built a key-value table that writes through an LSM tree and stores every sorted run
as an ordinary Iceberg data file in Avro, with a bloom filter and an exact key index in Iceberg's Puffin format.
Runs are merged level by level (compaction) with Iceberg rewrite commits. With {cfg['keys']:,} keys and {cfg['updates']:,}
streaming updates, a point lookup takes <b>{fmt(L['get_p50_ms'])} ms</b> and reads about one block, against
{fmt(ev['append']['get_p50_ms'])} ms for Iceberg append ({speed_vs_append:,.0f}× slower),
{fmt(ev['mor']['get_p50_ms'])} ms for merge-on-read and {fmt(ev['cow']['get_p50_ms'])} ms for copy-on-write.
Writes run at {fmt(L['write_ops_per_s'])} per second. The table stays a normal Iceberg table: Spark reads it, runs SQL
analytics on it and time-travels through its snapshots without any of our code.</div>

<h2>1. Problem</h2>
<p>Iceberg picks the files a query reads from the min and max of each column in each file. When updates stream in with
random keys, every new file covers almost the whole key range, so a lookup of one key opens every file. Iceberg's two
update modes trade this off badly: merge-on-read keeps writes cheap but readers must apply a growing pile of delete
files; copy-on-write keeps reads cheap but every batch rewrites most of the table. An LSM tree is the standard answer
for update-heavy key-value workloads. Our goal was to build one inside Iceberg's format, so the data stays readable by
every Iceberg engine.</p>

<h2>2. Design</h2>
<div class="arch">put/delete ─► WAL (CRC32 records) ─► MemTable (skip list) ──flush──► L0 run: sorted Avro file + Puffin index
                                                                        │   (one Iceberg append commit)
get(key) ─► MemTable ─► L0 runs, newest first ─► one run per level      ▼
              key range ─► bloom filter ─► key index ─► one block    L1 … Ln: sorted, non-overlapping runs
                                                                        ▲   (leveled merges = Iceberg rewrite commits)</div>
<ul>
  <li><b>Write path.</b> Each write gets a sequence number, goes to the write-ahead log, then to the in-memory skip list.
  A full memtable is written as one key-sorted Avro file (a <i>run</i>). One Iceberg transaction appends the file and
  moves the table property <code>lsm.manifest-location</code> to a new version of our level manifest (JSON), so the
  data and the LSM state change atomically.</li>
  <li><b>Avro runs.</b> We write the Avro blocks ourselves (about 64 KB each, uncompressed) and record the byte offset where
  each block starts, so a lookup seeks straight to one block and decodes it in one pass. We also store key, seq and op
  bounds in the Iceberg metadata, which Iceberg's own Avro writer does not do, so other engines can prune our files.</li>
  <li><b>Puffin index.</b> Each run's Puffin file holds three blobs: a bloom filter (1% false positives), an exact
  <b>key index</b> (open-addressing hash table: key fingerprint → block, row; 12 bytes per slot) and a block directory
  (first key, offset and row count of each block, for range scans). The Puffin file is attached to the run's snapshot
  with <code>updateStatistics()</code>. If it is lost, the index is rebuilt from the data file.</li>
  <li><b>Read path.</b> Memtable, then L0 runs newest first, then at most one run per deeper level (their key ranges don't
  overlap). In a run: range check, bloom filter, key index → read the one block and the row it names. A key the run
  doesn't hold costs no I/O. The first version found wins, and a delete marker (tombstone) hides older versions.</li>
  <li><b>Merges (compaction).</b> Leveled, as in RocksDB. When L0 has 4 runs, they are k-way merged with the overlapping
  L1 runs; a level over its size limit (10× the level above) pushes one run down, round-robin over the key space. Only
  the newest version of each key survives, and tombstones are dropped when no deeper level can hold the key. Each merge
  is one Iceberg <code>newRewrite()</code> commit (operation <code>replace</code>) that also moves the manifest
  pointer. Old files remain reachable from older snapshots.</li>
  <li><b>Range scans</b> k-way merge the memtable and the overlapping runs. <b>Crash recovery</b> replays log records
  newer than the last flushed sequence number.</li>
</ul>
<p>Iceberg (1.10) is used unmodified, only through its public Java API: catalog, transactions, append, rewrite,
table properties, snapshot summaries, Puffin and statistics files. About 3,400 lines of Java plus 400 lines of tests (19 automated tests).</p>

<h2>3. Changes after the checkpoint feedback</h2>
{table([
    ["Use Avro instead of Parquet (less decoding overhead)", "Runs are Avro by default; Parquet kept as an option so we could measure both."],
    ["Store a real index in Puffin instead of fence pointers + binary search", "New key-index blob: key → (block, row) hash table. Fence-pointer mode kept for comparison."],
    ["Implement merging", "Leveled compaction with Iceberg rewrite commits, tombstone and old-version cleanup."],
    ["Finish the project", "Three Iceberg baselines, YCSB workloads, full evaluation below."],
    ["Show it working with another system (Spark)", "Spark 3.5 reads the table through the official Iceberg runtime: analytics, metadata tables, time travel (Section 5)."],
], ["Feedback", "What we did"], numeric=False)}

<h2>4. Evaluation</h2>
<p><b>Setup.</b> {cfg['keys']:,} keys loaded in random order, then {cfg['updates']:,} skewed updates (10% deletes). Every table
commits a file every {int(cfg['batch']):,} writes, as a streaming job would. Lookups: {cfg['lookups']} keys, 80% existing (hot keys
more likely), 20% never written. Block cache off. MacBook Air, local SSD, one JVM; slow tables stop a phase after
{cfg['time-limit']} s and report what they finished. The <span class="sw" style="background:{BLUE}"></span>blue bars are our LSM
table and variants, the <span class="sw" style="background:{ORANGE}"></span>orange bars are plain Iceberg.</p>
<ul>
  <li><b>Iceberg append</b>: append each batch as a Parquet file; readers keep the newest version of each key.</li>
  <li><b>Iceberg merge-on-read</b>: each batch adds a data file and an equality-delete file on <code>key</code> (Flink upsert mode).</li>
  <li><b>Iceberg copy-on-write</b>: the table is kept as sorted, non-overlapping files; each batch rewrites the files it touches.</li>
</ul>
{charts}
{table(main_rows, ["Table", "Writes/s", "Rows written per write", "Bytes written per byte", "Files", "Get p50 ms", "Get p95 ms", "Files/get", "Scan p50 ms", "Answers differ"], "wide")}
<p class="cap">Rows written per write counts every row that lands in a data or delete file, rewrites included, per
write the user made; it does not depend on the file format. Bytes written per byte compares file bytes with the
bytes of keys and values written: our Avro runs are uncompressed while the Iceberg tables write zstd Parquet, so this
column also reflects compression. The WAL is not counted. "Answers differ" compares every table's lookup answers with ours.</p>

<h3>YCSB workloads (operations per second, zipfian keys)</h3>
{table(ycsb_rows, ["Table", "A: 50% read / 50% update", "B: 95% read", "C: 100% read", "E: 95% scans / 5% inserts"], "wide")}
<p class="cap">Each workload runs up to {int(cfg['ycsb-ops']):,} operations (or {cfg['time-limit']} s), and the final flush that makes its writes durable is included.</p>
{curve_chart()}

<h3>What each design choice buys (ablations)</h3>
{table(abl, ["Change", "Effect on reads", "Cost"], numeric=False)}
<p><b>Reading the results.</b> Lookups in plain Iceberg get slower as files pile up, because every file's key range
covers every key. Merge-on-read is the slowest reader, because every lookup also opens the delete files.
Copy-on-write reads one file, but it must scan that whole file, and every batch rewrites most of the table: its
batches write {fmt(ev['cow'].get('row_write_amplification'))} rows per user write against our
{fmt(L.get('row_write_amplification'))}, and that cost grows with the table, since a batch of random keys touches every file.
At our table size the rewrite is still cheap, which is why copy-on-write's write speed looks good here. Our table reads one
block of one file and stays flat as the table grows. Without merges our point lookups stay fast at this size, because the bloom filter and key index rule out
runs in memory. Merges matter for everything else: they cut our files from {ev.get('lsm-nocompact', L)['data_files']} to
{L['data_files']}, make range scans {ratio(ev.get('lsm-nocompact', L)['scan_p50_ms'], L['scan_p50_ms']):.0f}× faster, remove old versions and
deleted keys, and let other engines prune files (Section 5), at the price of writing each row about twice. Append is the fastest writer
because it does no sorting, no log and no indexing.</p>
<p>Merge totals for our table in this run: {ws.get('compactions', '–')} merges, {fmt(ws.get('versionsDropped', 0))} old
versions and {fmt(ws.get('tombstonesDropped', 0))} tombstones removed, {ws.get('compactionSeconds', '–')} s spent merging.
Final shape: {escape(str(L.get('levels', '')))}.</p>

<h2>5. Use case: a customer-profile store with Spark analytics</h2>
<p>An online shop streams change events (CDC) for {50000:,} customers: sign-ups, plan changes, city moves, purchases and
account closures, {200000:,} events in all. The current profile of each customer lives in one LSM-Iceberg table
(<code>./run.sh usecase</code>). Two kinds of users read the same table, with no copy in between:</p>
<ul>
  <li><b>The support app</b> looks customers up by id through our read path: p50
  {fmt(uc.get('lookup_ms_before_merge', {}).get('p50'))} ms before the merge and {fmt(uc.get('lookup_ms_after_merge', {}).get('p50'))} ms after it.
  Ingest ran at {fmt(uc.get('writes_per_s'))} writes per second.</li>
  <li><b>Analysts use Spark SQL</b> (<code>./run.sh spark</code>, Spark 3.5 with the Iceberg runtime 1.10). Spark sees the
  LSM history in the <code>snapshots</code> metadata table (flushes as <code>append</code>, merges as <code>replace</code>),
  builds a "current customers" view (newest <code>seq</code> per key, deletes dropped) and computes revenue by city and plan in
  {fmt(sp.get('analytics_query_s'))} s. Time travel (<code>VERSION AS OF</code>) shows the {fmt(sp.get('rows_before_merge'))}
  rows stored before the merge against {fmt(sp.get('rows_now'))} after it, for {fmt(sp.get('live_customers'))} live customers.</li>
  <li><b>Merges help Spark too.</b> Before the merge Iceberg plans {sp.get('files_planned_before_merge', uc.get('iceberg_files_planned_before_merge', '–'))}
  files for a lookup of one customer; after it, {sp.get('files_planned_after_merge', uc.get('iceberg_files_planned_after_merge', '–'))}. Merged runs
  have disjoint key ranges and our Avro files carry key bounds, so Iceberg's ordinary min/max pruning works for any engine.
  The Spark lookup took {fmt(sp.get('lookup_s_before_merge'))} s before and {fmt(sp.get('lookup_s_after_merge'))} s after
  (mostly Spark's fixed job overhead).</li>
</ul>

<h2>6. Related work</h2>
<p>Apache Paimon and Apache Hudi (merge-on-read tables) also bring LSM-style ingestion to lakehouse storage, but each
with its own table format. Iceberg's built-in answers are the merge-on-read and copy-on-write modes we compare against.
Iceberg's Puffin spec reserves room for indexes, and a proposal on the Iceberg dev list adds Puffin bloom filters for
file skipping during scan planning; released Iceberg (1.10) does not use them yet. Our work shows such an index, plus a
key → row index and leveled merges, working on the unmodified format.</p>

<h2>7. Limitations and future work</h2>
<ul>
  <li>One writer at a time; merges run inline after a flush, so a write that triggers a merge waits for it. A background
  merge thread is the next step.</li>
  <li>Spark reads the table but does not use our Puffin key index; doing that needs a change to Iceberg's scan planning
  (the dev-list proposal above) or a custom Spark data source.</li>
  <li>Snapshot expiry would delete the Puffin files of expired snapshots; our tables rebuild a missing index on open
  (tested), which is correct but slow. A production version would re-attach live indexes to the newest snapshot.</li>
  <li>Avro runs are uncompressed for lookup speed, so they take more space than zstd Parquet. A light codec is one setting away.</li>
  <li>Measured on a laptop with a local disk, not on object storage.</li>
</ul>

<h2>8. How to run</h2>
<pre>cd lsm-iceberg-java
./run.sh test        # 19 tests
./run.sh demo        # write path, traced lookups, merge, crash recovery
./run.sh inspect demo.kv warehouse/demo 3
./run.sh bench       # quick comparison (~2 min)     ./run.sh eval   # this report's numbers (~15 min)
./run.sh usecase     # CDC customer store            ./run.sh spark  # Spark SQL on it</pre>
</body></html>
"""

out = os.path.join(os.path.dirname(ROOT), "final_report.html")
open(out, "w").write(HTML)
print("wrote", out)
