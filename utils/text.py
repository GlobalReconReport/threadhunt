"""
Text utility functions — no external deps beyond stdlib.
"""
import re
import math


def username_entropy(username: str) -> float:
    """Shannon entropy of username character set. Higher = more random = more bot-like."""
    if not username:
        return 0.0
    freq: dict = {}
    for c in username.lower():
        freq[c] = freq.get(c, 0) + 1
    total = len(username)
    entropy = 0.0
    for count in freq.values():
        p = count / total
        if p > 0:
            entropy -= p * math.log2(p)
    return entropy


def has_digit_suffix(username: str) -> bool:
    """Detect common bot pattern: name followed by 4+ digits."""
    return bool(re.search(r'[a-zA-Z]\d{4,}$', username))


def digit_ratio(username: str) -> float:
    """Fraction of username characters that are digits."""
    if not username:
        return 0.0
    digits = sum(1 for c in username if c.isdigit())
    return digits / len(username)


def looks_random(username: str) -> bool:
    """Heuristic: high entropy + digit suffix => likely generated username."""
    return username_entropy(username) > 3.2 or has_digit_suffix(username) or digit_ratio(username) > 0.4


def strip_html(html: str) -> str:
    """Remove HTML tags, collapse whitespace."""
    text = re.sub(r'<[^>]+>', ' ', html)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'&#\d+;', '', text)
    return re.sub(r'\s+', ' ', text).strip()


def normalize_text(text: str) -> str:
    """Lowercase + collapse whitespace."""
    return re.sub(r'\s+', ' ', text.lower()).strip()


def extract_hashtags(text: str) -> list:
    return re.findall(r'#(\w+)', text.lower())


def extract_mentions(text: str) -> list:
    return re.findall(r'@(\w+)', text.lower())


def extract_urls(text: str) -> list:
    return re.findall(r'https?://[^\s<>"\']+', text)


def posts_per_hour(timestamps: list) -> float:
    """
    Given a list of ISO timestamp strings, compute average posts/hour
    over the span. Returns 0.0 if < 2 posts.
    """
    if len(timestamps) < 2:
        return 0.0
    parsed = []
    for ts in timestamps:
        dt = _parse_iso(ts)
        if dt is not None:
            parsed.append(dt)
    if len(parsed) < 2:
        return 0.0
    parsed.sort()
    span_seconds = (parsed[-1] - parsed[0]).total_seconds()
    if span_seconds <= 0:
        return 0.0
    span_hours = span_seconds / 3600.0
    return len(parsed) / span_hours


def _parse_iso(ts: str):
    """Parse ISO timestamp, return datetime or None."""
    from datetime import datetime, timezone
    if not ts:
        return None
    ts = ts.rstrip('Z')
    # Handle +00:00 suffix
    if '+' in ts[10:]:
        ts = ts[:ts.rfind('+')]
    formats = [
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%dT%H:%M',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d',
    ]
    for fmt in formats:
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    return None
