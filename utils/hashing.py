"""
SimHash fingerprinting — 64-bit, scratch implementation.
No vocabulary storage. Stored as INTEGER in SQLite.
"""
import hashlib
import re


def simhash(text: str, bits: int = 64) -> int:
    """
    64-bit SimHash fingerprint of text.
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

    return fingerprint


def hamming_distance(a: int, b: int) -> int:
    """Bit count of XOR — number of differing bits."""
    return bin(a ^ b).count('1')


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
