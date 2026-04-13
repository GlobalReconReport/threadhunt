"""
Optional Tor routing via SOCKS5 on localhost:9050.
Kali Linux ships with Tor — just `sudo service tor start`.
"""
import socket
import logging
import requests

logger = logging.getLogger('threadhunt')

TOR_PROXY = {
    'http':  'socks5h://127.0.0.1:9050',
    'https': 'socks5h://127.0.0.1:9050',
}

DEFAULT_HEADERS = {
    'User-Agent': (
        'Mozilla/5.0 (X11; Linux x86_64; rv:109.0) '
        'Gecko/20100101 Firefox/115.0'
    ),
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'DNT': '1',
}


def is_tor_running() -> bool:
    """Check if Tor SOCKS5 proxy is accepting connections on :9050."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2)
        result = s.connect_ex(('127.0.0.1', 9050))
        s.close()
        return result == 0
    except Exception:
        return False


def get_session(use_tor: bool = False) -> requests.Session:
    """
    Build a requests Session with correct headers.
    If use_tor=True and Tor is running, route through SOCKS5.
    """
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)

    if use_tor:
        if is_tor_running():
            session.proxies.update(TOR_PROXY)
            logger.info("Routing through Tor (SOCKS5 :9050)")
        else:
            logger.warning("Tor requested but :9050 not reachable — using direct connection")

    return session
