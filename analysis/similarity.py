"""
Language detection and optional translation.
Primary: langdetect (offline, no key needed).
Secondary: LibreTranslate (local instance if running on :5000).
Tertiary: openai_nlp.translate_text (if API key configured).
"""
import logging

logger = logging.getLogger('threadhunt')

LIBRETRANSLATE_URL = 'http://127.0.0.1:5000/translate'


# ── Language detection ────────────────────────────────────────────────────────

def detect_language(text: str) -> str:
    """
    Detect language of text using langdetect.
    Returns ISO 639-1 code (e.g. 'en', 'ru', 'de') or 'unknown'.
    """
    if not text or len(text.strip()) < 10:
        return 'unknown'
    try:
        from langdetect import detect, LangDetectException
        return detect(text)
    except Exception:
        return 'unknown'


def detect_language_with_confidence(text: str) -> tuple:
    """
    Returns (lang_code, confidence_float).
    Uses langdetect's probability list.
    """
    if not text or len(text.strip()) < 10:
        return ('unknown', 0.0)
    try:
        from langdetect import detect_langs
        results = detect_langs(text)
        if results:
            top = results[0]
            return (str(top.lang), float(top.prob))
    except Exception:
        pass
    return ('unknown', 0.0)


def tag_posts_language(conn, batch_size: int = 200) -> int:
    """
    Detect and update language for posts where lang IS NULL.
    Returns count of posts tagged.
    Processed in batches to avoid memory pressure.
    """
    tagged = 0
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, content FROM posts
        WHERE lang IS NULL AND content IS NOT NULL
        LIMIT 5000
    """)

    while True:
        rows = cursor.fetchmany(batch_size)
        if not rows:
            break
        for row in rows:
            post_id, content = row[0], row[1]
            if content:
                lang = detect_language(content)
                conn.execute("UPDATE posts SET lang=? WHERE id=?",
                             (lang, post_id))
                tagged += 1

    logger.info("Language tagging: %d posts updated", tagged)
    return tagged


# ── Translation ───────────────────────────────────────────────────────────────

def translate(session, text: str, target_lang: str = 'en',
              source_lang: str = 'auto') -> str:
    """
    Translate text to target_lang.
    Tries LibreTranslate (local), then OpenAI (if key set).
    Returns original text if all methods fail.
    """
    if not text.strip():
        return text

    # Method 1: LibreTranslate local instance
    result = _libretranslate(session, text, source_lang, target_lang)
    if result:
        return result

    # Method 2: OpenAI (if configured)
    try:
        from collectors.with_api.openai_nlp import translate_text, is_available
        if is_available():
            return translate_text(session, text, target_lang)
    except Exception:
        pass

    return text   # Return original if nothing works


def _libretranslate(session, text: str, source: str, target: str) -> str | None:
    """
    POST to a local LibreTranslate instance.
    Returns translated string or None.
    LibreTranslate can be installed: pip install libretranslate && libretranslate
    """
    try:
        import json
        r = session.post(
            LIBRETRANSLATE_URL,
            data=json.dumps({
                'q':      text[:2000],
                'source': source,
                'target': target,
                'format': 'text',
            }),
            headers={'Content-Type': 'application/json'},
            timeout=5,
        )
        if r.status_code == 200:
            return r.json().get('translatedText', '')
    except Exception:
        pass
    return None


def is_libretranslate_available(session) -> bool:
    """Check if a local LibreTranslate instance is up."""
    try:
        r = session.get('http://127.0.0.1:5000/languages', timeout=2)
        return r.status_code == 200
    except Exception:
        return False


# ── Language distribution report ─────────────────────────────────────────────

def language_distribution(conn, platform: str = None, limit: int = 20) -> list:
    """
    Return top languages in the posts table.
    Returns list of {lang, count} sorted by count desc.
    """
    query = """
        SELECT lang, COUNT(*) as cnt FROM posts
        WHERE lang IS NOT NULL AND lang != 'unknown'
        {where}
        GROUP BY lang ORDER BY cnt DESC LIMIT ?
    """
    if platform:
        q = query.format(where="AND platform=?")
        rows = conn.execute(q, (platform, limit)).fetchall()
    else:
        q = query.format(where='')
        rows = conn.execute(q, (limit,)).fetchall()

    return [{'lang': row[0], 'count': row[1]} for row in rows]
