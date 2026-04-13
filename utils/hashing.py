"""
SimHash fingerprinting — 64-bit, scratch implementation.
No vocabulary storage. Stored as INTEGER in SQLite.
"""
import hashlib
import re


def simhash(text: str, bits: int = 64) -> int:
    """
    64-bit SimHash fingerprint of text, stored as a SIGNED 64-bit integer.

    SQLite INTEGER is signed 64-bit (-2^63 to 2^63-1). We compute the
    fingerprint as unsigned, then reinterpret as two's complement so it
    fits in a SQLite INTEGER column without overflow.

    Hamming distance <= 5 => near-duplicate.
    """
    if not text or not text.strip():
        return 0

    tokens = _tokenize(text)
    if not tokens:
        return 0

    v = [0] * bits

    for token in tokens:
        h = int(hashlib.md5(token.encode('utf-8', errors='replace')).hexdigest(), 16)
        for i in range(bits):
            if (h >> i) & 1:
                v[i] += 1
            else:
                v[i] -= 1

    fingerprint = 0
    for i in range(bits):
        if v[i] > 0:
            fingerprint |= (1 << i)

    # Reinterpret as signed two's complement so value fits in SQLite INTEGER.
    # Values in [2^63, 2^64-1] map to [-2^63, -1] preserving all bit patterns.
    if fingerprint >= (1 << 63):
        fingerprint -= (1 << 64)

    return fingerprint


def hamming_distance(a: int, b: int) -> int:
    """
    Bit count of differing bits between two 64-bit SimHash values.
    Handles signed integers (stored as SQLite INTEGER) by masking to 64 bits
    before XOR so negative values compare correctly.
    """
    _MASK64 = (1 << 64) - 1
    return bin((a & _MASK64) ^ (b & _MASK64)).count('1')


def _tokenize(text: str) -> list:
    """Minimal tokenizer. Strips URLs/mentions, extracts alpha-numeric tokens ≥3 chars."""
    text = text.lower()
    text = re.sub(r'https?://\S+', '', text)
    text = re.sub(r'[@#]\w+', '', text)
    return re.findall(r'\b[a-z0-9]{3,}\b', text)


def profile_pic_hash(image_bytes: bytes) -> str:
    """MD5 of raw image bytes for identity/reuse detection."""
    return hashlib.md5(image_bytes).hexdigest()


def content_hash(text: str) -> str:
    """SHA-256 of post content for exact-duplicate detection."""
    return hashlib.sha256(text.encode('utf-8', errors='replace')).hexdigest()
