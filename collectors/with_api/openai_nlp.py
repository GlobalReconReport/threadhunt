"""
OpenAI NLP module — DISABLED until API key is configured.
Activate: threadhunt config --set-key openai_api_key <key>

Provides GPT-based narrative summarization and campaign theme extraction.
Without this module, all analysis is heuristic-only (which is sufficient
for detection — this adds human-readable summaries for reporting).
"""
import logging
import json

import config

logger = logging.getLogger('threadhunt')

COMPLETIONS_URL = 'https://api.openai.com/v1/chat/completions'
MODEL = 'gpt-4o-mini'   # Cheapest capable model


def is_available() -> bool:
    return config.has_api_key('openai_api_key')


def _headers() -> dict:
    return {
        'Authorization': f'Bearer {config.get_api_key("openai_api_key")}',
        'Content-Type': 'application/json',
    }


def _chat(session, messages: list, max_tokens: int = 500) -> str | None:
    """Send a chat completion request. Returns response text or None."""
    if not is_available():
        return None

    payload = {
        'model': MODEL,
        'messages': messages,
        'max_tokens': max_tokens,
        'temperature': 0.3,
    }
    try:
        r = session.post(COMPLETIONS_URL, headers=_headers(),
                         data=json.dumps(payload),
                         timeout=30)
        if r.status_code == 200:
            return r.json()['choices'][0]['message']['content'].strip()
        logger.warning("OpenAI HTTP %d: %s", r.status_code, r.text[:200])
    except Exception as e:
        logger.error("OpenAI request error: %s", e)
    return None


def summarize_campaign(session, posts: list, keyword: str = None) -> str:
    """
    Summarize a cluster of posts as a narrative threat summary.
    posts: list of content strings (max 20 used to avoid token cost).
    Returns a plain-text summary or empty string if unavailable.
    """
    if not is_available():
        return ''

    sample = posts[:20]
    posts_text = '\n---\n'.join(sample)

    system_msg = (
        "You are an intelligence analyst specializing in disinformation and extremism. "
        "Summarize the coordinated messaging campaign represented by these posts. "
        "Identify: the core narrative, likely target audience, emotional manipulation tactics, "
        "and whether the content appears to be machine-generated or human-written. "
        "Be concise (3–5 sentences). Do not editorialize."
    )
    user_msg = f"Keyword of interest: {keyword or 'N/A'}\n\nPosts:\n{posts_text}"

    return _chat(session, [
        {'role': 'system', 'content': system_msg},
        {'role': 'user',   'content': user_msg},
    ], max_tokens=300) or ''


def classify_language_register(session, text: str) -> str:
    """
    Classify the register/style of a post: formal, informal, propaganda,
    bot-generated, emotional-manipulation, etc.
    Returns a short label or empty string if unavailable.
    """
    if not is_available():
        return ''

    return _chat(session, [
        {'role': 'system', 'content':
            'Classify the rhetorical register of this text in 1–3 words. '
            'Examples: propaganda, bot-generated, grassroots, emotional-bait, '
            'disinformation, organic commentary.'},
        {'role': 'user', 'content': text[:500]},
    ], max_tokens=20) or ''


def translate_text(session, text: str, target_lang: str = 'en') -> str:
    """
    Translate text to target_lang using GPT. Used when LibreTranslate is unavailable.
    Returns translated text or original on failure.
    """
    if not is_available():
        return text

    result = _chat(session, [
        {'role': 'system', 'content':
            f'Translate the following to {target_lang}. '
            f'Return only the translation, no explanation.'},
        {'role': 'user', 'content': text[:1000]},
    ], max_tokens=400)

    return result or text
