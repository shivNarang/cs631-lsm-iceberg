"""Phase 1: walk an Iceberg table's metadata tree.

table metadata -> snapshots -> manifest list -> manifests -> data files (+ min/max bounds)
                            -> statistics (Puffin) files
Usage: python scripts/inspect_metadata.py <namespace.table> [--warehouse DIR]
"""
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from lsm_iceberg import get_catalog  # noqa: E402


def short(p, n=70):
    p = str(p)
    return p if len(p) <= n else "..." + p[-(n - 3):]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("table")
    ap.add_argument("--warehouse")
    ap.add_argument("--max-snapshots", type=int, default=5)
    a = ap.parse_args()
    t = get_catalog(a.warehouse).load_table(a.table)
    md = t.metadata
    print(f"TABLE {a.table}  (format v{md.format_version})")
    print(f"  location     : {short(t.location())}")
    print(f"  metadata file: {short(t.metadata_location)}")
    print(f"  schema       : {', '.join(f'{f.name}:{f.field_type}' for f in t.schema().fields)}")
    print(f"  sort order   : {t.sort_order()}")
    print("  properties   :")
    for k, v in sorted(t.properties.items()):
        print(f"     {k} = {short(v, 60)}")
    stats_by_snap = {s.snapshot_id: s for s in md.statistics}
    snaps = md.snapshots[-a.max_snapshots:]
    print(f"\n  {len(md.snapshots)} snapshots (showing last {len(snaps)}):")
    for s in snaps:
        extra = {k: v for k, v in s.summary.additional_properties.items() if k.startswith("lsm.")}
        print(f"\n  SNAPSHOT {s.snapshot_id}  seq#{s.sequence_number}  op={s.summary.operation.value}  {extra}")
        print(f"    manifest list: {short(s.manifest_list)}")
        for m in s.manifests(t.io):
            print(f"    MANIFEST {short(m.manifest_path, 60)}  (added files: {m.added_files_count})")
            for e in m.fetch_manifest_entry(t.io, discard_deleted=True):
                df = e.data_file
                lo = df.lower_bounds.get(1, b"").decode(errors="replace")
                hi = df.upper_bounds.get(1, b"").decode(errors="replace")
                print(f"      DATA FILE {os.path.basename(df.file_path):28s} rows={df.record_count:<7} "
                      f"key min/max=[{lo}, {hi}]")
        if s.snapshot_id in stats_by_snap:
            st = stats_by_snap[s.snapshot_id]
            print(f"    PUFFIN INDEX {os.path.basename(st.statistics_path)} ({st.file_size_in_bytes} bytes)")


if __name__ == "__main__":
    main()
