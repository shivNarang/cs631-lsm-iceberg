"""Minimal reader/writer for Iceberg's Puffin file format (uncompressed blobs).

Layout (from the Iceberg Puffin spec):
    Magic Blob_1 ... Blob_n Footer
    Footer = Magic FooterPayload(JSON) FooterPayloadSize(int32 LE) Flags(4 bytes) Magic
"""
import json
import struct
from dataclasses import dataclass, field
from typing import Dict, List, Tuple

MAGIC = b"PFA1"


@dataclass
class Blob:
    type: str
    fields: List[int]
    data: bytes
    snapshot_id: int = -1
    sequence_number: int = -1
    properties: Dict[str, str] = field(default_factory=dict)


def write_puffin(path: str, blobs: List[Blob], properties: Dict[str, str] = None) -> Tuple[int, int, List[dict]]:
    """Write blobs; returns (file_size, footer_size, blob metadata dicts)."""
    meta = []
    with open(path, "wb") as f:
        f.write(MAGIC)
        offset = len(MAGIC)
        for b in blobs:
            f.write(b.data)
            meta.append({
                "type": b.type,
                "fields": b.fields,
                "snapshot-id": b.snapshot_id,
                "sequence-number": b.sequence_number,
                "offset": offset,
                "length": len(b.data),
                "properties": b.properties,
            })
            offset += len(b.data)
        payload = json.dumps({"blobs": meta, "properties": properties or {"created-by": "cs631-lsm-iceberg"}}).encode()
        footer = MAGIC + payload + struct.pack("<i", len(payload)) + b"\x00\x00\x00\x00" + MAGIC
        f.write(footer)
    return offset + len(footer), len(footer), meta


def read_puffin(path: str) -> Dict[str, Tuple[dict, bytes]]:
    """Return {blob_type: (blob_metadata, blob_bytes)}."""
    with open(path, "rb") as f:
        data = f.read()
    if data[:4] != MAGIC or data[-4:] != MAGIC:
        raise ValueError(f"{path} is not a Puffin file")
    (size,) = struct.unpack("<i", data[-12:-8])
    footer_start = len(data) - 12 - size - 4
    if data[footer_start:footer_start + 4] != MAGIC:
        raise ValueError(f"{path}: corrupt Puffin footer")
    payload = json.loads(data[footer_start + 4:footer_start + 4 + size])
    out = {}
    for m in payload["blobs"]:
        out[m["type"]] = (m, data[m["offset"]:m["offset"] + m["length"]])
    return out
