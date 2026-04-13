"""
Shodan infrastructure mapper — DISABLED until API key is configured.
Activate: threadhunt config --set-key shodan_api_key <key>

Used to pivot from identified bot networks to hosting infrastructure:
IP ranges, ASNs, hosting providers, co-located domains.
Useful for attributing coordinated accounts to a common origin.
"""
import logging

import config

logger = logging.getLogger('threadhunt')

SEARCH_URL   = 'https://api.shodan.io/shodan/host/search'
HOST_URL     = 'https://api.shodan.io/shodan/host/{ip}'
DNS_URL      = 'https://api.shodan.io/dns/resolve'


def is_available() -> bool:
    return config.has_api_key('shodan_api_key')


def _key() -> str:
    return config.get_api_key('shodan_api_key') or ''


def lookup_ip(session, ip: str) -> dict:
    """
    Fetch Shodan host data for an IP address.
    Returns dict with ports, services, org, country, hostnames.
    """
    if not is_available():
        logger.info("Shodan: API key not configured")
        return {}

    try:
        r = session.get(HOST_URL.format(ip=ip),
                        params={'key': _key()},
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            data = r.json()
            return {
                'ip':        ip,
                'org':       data.get('org', ''),
                'country':   data.get('country_name', ''),
                'asn':       data.get('asn', ''),
                'hostnames': data.get('hostnames', []),
                'ports':     data.get('ports', []),
                'tags':      data.get('tags', []),
            }
        logger.warning("Shodan HTTP %d for %s", r.status_code, ip)
    except Exception as e:
        logger.error("Shodan lookup error: %s", e)
    return {}


def search(session, query: str, limit: int = 20) -> list:
    """
    Run a Shodan search query.
    Returns list of {ip, org, country, ports, hostnames}.
    """
    if not is_available():
        return []

    results = []
    try:
        r = session.get(SEARCH_URL,
                        params={'key': _key(), 'query': query},
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            for match in r.json().get('matches', [])[:limit]:
                results.append({
                    'ip':        match.get('ip_str', ''),
                    'org':       match.get('org', ''),
                    'country':   match.get('location', {}).get('country_name', ''),
                    'ports':     [match.get('port')],
                    'hostnames': match.get('hostnames', []),
                })
    except Exception as e:
        logger.error("Shodan search error: %s", e)
    return results


def resolve_domains(session, domains: list) -> dict:
    """
    Bulk DNS resolution via Shodan.
    Returns {domain: ip, ...}.
    """
    if not is_available() or not domains:
        return {}

    try:
        r = session.get(DNS_URL,
                        params={'key': _key(), 'hostnames': ','.join(domains[:100])},
                        timeout=config.get('request_timeout', 10))
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        logger.error("Shodan DNS resolve error: %s", e)
    return {}
