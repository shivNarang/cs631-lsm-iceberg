"""Bloom filter used to skip runs that definitely don't contain a key."""
import hashlib
import math
import struct

_HEADER = struct.Struct("<QI")  # num_bits, num_hashes


class BloomFilter:
    def __init__(self, num_bits: int, num_hashes: int, bits: bytearray = None):
        self.num_bits = max(8, num_bits)
        self.num_hashes = max(1, num_hashes)
        self.bits = bits if bits is not None else bytearray((self.num_bits + 7) // 8)

    @classmethod
    def for_capacity(cls, n: int, fpr: float) -> "BloomFilter":
        n = max(1, n)
        m = math.ceil(-n * math.log(fpr) / (math.log(2) ** 2))
        k = max(1, round(m / n * math.log(2)))
        return cls(m, k)

    def _positions(self, key: str):
        d = hashlib.blake2b(key.encode(), digest_size=16).digest()
        h1 = int.from_bytes(d[:8], "little")
        h2 = int.from_bytes(d[8:], "little") | 1
        m = self.num_bits
        return [(h1 + i * h2) % m for i in range(self.num_hashes)]

    def add(self, key: str) -> None:
        for p in self._positions(key):
            self.bits[p >> 3] |= 1 << (p & 7)

    def might_contain(self, key: str) -> bool:
        bits = self.bits
        return all(bits[p >> 3] & (1 << (p & 7)) for p in self._positions(key))

    def to_bytes(self) -> bytes:
        return _HEADER.pack(self.num_bits, self.num_hashes) + bytes(self.bits)

    @classmethod
    def from_bytes(cls, data: bytes) -> "BloomFilter":
        m, k = _HEADER.unpack_from(data)
        return cls(m, k, bytearray(data[_HEADER.size:]))
