"""
Configuration management. Persisted to data/config.json.
API keys are optional slots — tool stays fully functional without them.
"""
import json
import logging
from pathlib import Path

logger = logging.getLogger('threadhunt')

DATA_DIR = Path(__file__).parent / 'data'
CONFIG_FILE = DATA_DIR / 'config.json'

DEFAULT_CONFIG = {
    # Detection thresholds
    'bot_score_threshold':          0.7,
    'campaign_min_accounts':        3,
    'campaign_time_window_minutes': 30,
    'campaign_simhash_distance':    5,

    # Narrative clustering (semantic coordination detection)
    'narrative_time_window_hours':   6,    # time window for clustering
    'narrative_min_keyword_overlap': 3,    # shared keywords required
    'narrative_min_sources':         3,    # distinct accounts (per-platform)
    'narrative_min_platforms':       2,    # distinct platforms (cross-platform)

    # Collector caps
    'collector_post_cap':   500,
    'fourchan_post_cap':    1000,

    # Watch mode
    'watch_refresh_seconds': 60,

    # Networking
    'use_tor': False,
    'request_timeout': 10,

    # Logging
    'verbose': False,

    # API keys — all None = disabled
    'api_keys': {
        'twitter_bearer_token': None,
        'openai_api_key':       None,
        'shodan_api_key':       None,
    },

    # Nitter instance pool (health-checked on startup)
    'nitter_instances': [
        'https://nitter.net',
        'https://nitter.nl',
        'https://nitter.1d4.us',
        'https://nitter.kavin.rocks',
        'https://nitter.unixfox.eu',
        'https://nitter.42l.fr',
        'https://nitter.pussthecat.org',
        'https://nitter.fdn.fr',
    ],

    # 4chan boards to monitor by default
    'fourchan_boards': ['pol', 'k', 'int'],

    # Telegram: curated list of known Russian state-adjacent and disinfo channels.
    # Used when `collect --platform telegram` is run without --target.
    # rybar/grey_zone/readovka = pro-Kremlin milbloggers with high amplification reach
    # war_monitor/kremlin_inform = aggregators for Russian military narrative
    # rt_general/sputnik_int = state media English-language outlets
    'telegram_channels': [
        'rybar',
        'grey_zone',
        'readovkanews',
        'war_monitor',
        'kremlin_inform',
        'rt_general',
        'sputnik_int',
    ],

    # VK: known Russian state media groups.
    # Used when `collect --platform vk` is run without --target.
    'vk_groups': [
        'rt_russian',
        'ria_novosti',
        'tass_agency',
        'sputnik_russia',
    ],

    # YouTube: channels to monitor by default (no --target required).
    # Mix of Western-facing Russian state media and relevant disinfo vectors.
    'youtube_channels': [
        'TheGrayzone',
        'RT',
        'DDGeopolitics',
    ],
}

_config: dict | None = None


def load_config() -> dict:
    global _config
    if _config is not None:
        return _config

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE) as f:
                stored = json.load(f)
            merged = dict(DEFAULT_CONFIG)
            merged.update({k: v for k, v in stored.items() if k != 'api_keys'})
            if 'api_keys' in stored:
                merged['api_keys'] = {**DEFAULT_CONFIG['api_keys'], **stored['api_keys']}
            _config = merged
            return _config
        except Exception as e:
            logger.warning("Config load failed (%s) — using defaults", e)

    _config = dict(DEFAULT_CONFIG)
    _config['api_keys'] = dict(DEFAULT_CONFIG['api_keys'])
    _save()
    return _config


def _save():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(CONFIG_FILE, 'w') as f:
        json.dump(_config, f, indent=2)


def get(key: str, default=None):
    return load_config().get(key, default)


def set_value(name: str, value: str):
    """Set a top-level config key or an api_keys sub-key."""
    cfg = load_config()

    if name in cfg.get('api_keys', {}):
        cfg['api_keys'][name] = value if value.lower() != 'none' else None
    else:
        # Coerce type based on current value type
        current = cfg.get(name)
        if isinstance(current, bool):
            cfg[name] = value.lower() in ('true', '1', 'yes')
        elif isinstance(current, int):
            try:
                cfg[name] = int(value)
            except ValueError:
                cfg[name] = value
        elif isinstance(current, float):
            try:
                cfg[name] = float(value)
            except ValueError:
                cfg[name] = value
        else:
            cfg[name] = value

    _save()
    global _config
    _config = cfg
    logger.info("Config updated: %s", name)


def has_api_key(name: str) -> bool:
    cfg = load_config()
    return bool(cfg.get('api_keys', {}).get(name))


def get_api_key(name: str) -> str | None:
    cfg = load_config()
    return cfg.get('api_keys', {}).get(name)
