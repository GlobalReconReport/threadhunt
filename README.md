# THREADHUNT

```
 _____ _                        _   _   _             _
|_   _| |__  _ __ ___  __ _  __| | | | | |_   _ _ __ | |_
  | | | '_ \| '__/ _ \/ _` |/ _` | | |_| | | | | '_ \| __|
  | | | | | | | |  __/ (_| | (_| | |  _  | |_| | | | | |_
  |_| |_| |_|_|  \___|\__,_|\__,_| |_| |_|\__,_|_| |_|\__|

  [ COORDINATED THREAT DETECTION ]  [ OSINT PLATFORM ]  [ FIELD EDITION ]
```

**Field-deployable OSINT terminal for detecting coordinated extremism and foreign disinformation campaigns.**

THREADHUNT is a lightweight, terminal-only intelligence platform built for analysts working in constrained environments — Kali Linux Live USB, low RAM, no persistent storage. It collects public data from social media platforms without API keys, detects coordinated inauthentic behavior using SimHash clustering and heuristic bot scoring, and generates structured intelligence reports — all from a single SQLite database with no external dependencies beyond five Python packages.

---

## Features

| Capability | Detail |
|---|---|
| **SimHash Campaign Detection** | Clusters near-duplicate posts across accounts within configurable time windows. Flags coordinated inauthentic behavior when ≥3 unique accounts push similar content. |
| **Heuristic Bot Scoring** | Five-factor 0.0–1.0 scoring: username randomness, follower/following ratio, post frequency, profile image reuse, cross-account content duplication. No ML required. |
| **Multi-Platform Collectors** | Nitter (Twitter/X), 4chan JSON API, Telegram public channels, VK public walls, generic web scraper with Wayback Machine archiving. |
| **Temporal Analysis** | Posting hour distribution, Shannon entropy scoring, synchronized activity burst detection, timezone inference from behavioral patterns. |
| **Identity Linking** | Cross-platform username similarity (Levenshtein), shared profile image hash detection, posting time correlation across accounts. |
| **Real-Time Watch Mode** | Live DB dashboard with keyboard navigation. Refreshes on configurable interval. Read-only — never triggers collection or analysis. |
| **State Export / Import** | Pack full DB + config to `.tar.gz` before reboot. Restore on next session. Built for Live USB workflows where RAM is wiped on shutdown. |
| **Alert System** | Four trigger types: coordinated campaign, high bot score, narrative re-emergence (keyword silent 48h then reappears), keyword spike (3× baseline in 30 min). |
| **Optional API Enhancement** | Twitter v2, OpenAI (GPT narrative summaries), Shodan (infrastructure pivoting). All dormant until keys are configured. |

---

## Requirements

- **Python** 3.10 or higher
- **OS** Kali Linux recommended; works on any Debian/Ubuntu system
- **RAM** 2 GB minimum, 4 GB recommended
- **Storage** ~50 MB for core install + ~200 MB if Playwright/Firefox is enabled
- **Network** Internet access for collection; Tor supported via SOCKS5

### Python dependencies (auto-installed)

```
rich>=13.0.0
requests>=2.28.0
beautifulsoup4>=4.11.0
langdetect>=1.0.9
python-Levenshtein>=0.20.0
```

No pandas. No numpy. No ML frameworks. Designed for minimal resource footprint.

### Optional: Playwright (enhanced Nitter collection)

```bash
pip install playwright
playwright install firefox   # downloads ~200 MB Firefox binary
```

When installed, THREADHUNT automatically uses a headless Firefox browser to reach
Nitter instances protected by Cloudflare or other JS challenges (e.g. `nitter.net`)
that return empty bodies to plain HTTP scrapers. The `install.sh` script prompts
whether to install it during setup.

**What it enables:**
- Bypasses JS challenges on Cloudflare-protected Nitter instances
- Search endpoint support (`from:{username}` and keyword queries) on instances
  that work in a browser but block requests-based scraping
- Falls back transparently — all existing requests-based paths still run first

**Without Playwright**, Nitter collection uses the requests path + Twitter guest
API + CDN syndication fallbacks. Most accounts will still be reachable.

---

## Installation

### Quick install (recommended)

```bash
git clone https://github.com/GlobalReconReport/threadhunt.git
cd threadhunt
chmod +x install.sh
./install.sh
```

The install script will check your Python version, install all dependencies, initialize the database, and confirm first-run instructions.

### Manual install

```bash
git clone https://github.com/GlobalReconReport/threadhunt.git
cd threadhunt

# Optional: virtual environment
python3 -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python main.py db-init
```

### Kali Linux (PEP 668)

Kali 2024+ enforces system package isolation. Use either:

```bash
# Option A — virtual environment (recommended)
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Option B — override system isolation
pip install -r requirements.txt --break-system-packages
```

---

## First Run

### Step 1 — Initialize the database

```bash
python main.py db-init
```

Creates `data/threadhunt.db` with the full schema. Safe to run multiple times.

### Step 2 — Add watchlist keywords

```bash
python main.py watch-add "great replacement"
python main.py watch-add "replacement theory"
python main.py watch-add "#WhiteGenocide"     --type hashtag
python main.py watch-add "InfoWars"           --type keyword --platform nitter
python main.py watch-add "rt_russian"         --type account --platform vk
```

Watchlist keywords are used by collectors to filter relevant content, and by the alert system to detect spikes and re-emergence.

### Step 3 — Collect from platforms

```bash
# Twitter/X via Nitter (no API key)
python main.py collect --platform nitter --target someaccount

# 4chan /pol/ board (monitors catalog against watchlist)
python main.py collect --platform 4chan --target pol

# Telegram public channel
python main.py collect --platform telegram --target someChannel

# VK public group
python main.py collect --platform vk --target some_group

# Generic URL with Wayback Machine archiving
python main.py collect --platform web --target https://example.com/article
```

### Step 4 — Run analysis

```bash
python main.py analyze
```

Runs the full pipeline: bot scoring → campaign detection → language tagging → alert triggers.

### Step 5 — Check alerts

```bash
python main.py alert --unread
```

### Step 6 — Review campaigns

```bash
python main.py report
```

### Step 7 — Export state before shutdown

**Critical on Live USB — your RAM will be wiped on reboot.**

```bash
python main.py export-state
# Saves: threadhunt_state_YYYYMMDD_HHMMSS.tar.gz
# Copy this file to persistent storage before powering off.
```

Restore on next session:

```bash
python main.py import-state threadhunt_state_20241215_143022.tar.gz
```

---

## Command Reference

### `db-init`

Initialize the SQLite database. Run once on first install, or after `import-state`.

```bash
python main.py db-init
```

---

### `collect`

Collect posts from a platform target.

```bash
python main.py collect --platform <platform> --target <target> [--keyword <kw>] [--verbose]
```

| Platform | Target | Example |
|---|---|---|
| `nitter` | Twitter username | `--target elonmusk` |
| `4chan` | Board name | `--target pol` |
| `telegram` | Channel username | `--target durov` |
| `vk` | Group short name | `--target rt_russian` |
| `web` | Full URL | `--target https://example.com` |

**Examples:**

```bash
# Collect up to 500 posts from a Twitter account via Nitter
python main.py collect --platform nitter --target someaccount

# Collect from 4chan /pol/ and filter to posts mentioning a keyword
python main.py collect --platform 4chan --target pol --keyword "replacement"

# Collect a Telegram channel with live progress output
python main.py collect --platform telegram --target some_channel --verbose

# Archive a page before it gets taken down
python main.py collect --platform web --target https://example.com/target-page
```

**Caps** (configurable via `config`):
- Nitter / Telegram / VK / Web: 500 posts per run
- 4chan: 1000 posts per run

---

### `analyze`

Run the full analysis pipeline against collected data.

```bash
python main.py analyze
python main.py analyze --platform nitter
python main.py analyze --time-window 24h
```

Pipeline steps:
1. **Bot scoring** — recomputes `bot_score` for every account
2. **Campaign detection** — SimHash clustering within time window
3. **Language tagging** — detects language for untagged posts
4. **Alert triggers** — checks all four alert conditions

---

### `alert`

View and manage alerts.

```bash
# All alerts
python main.py alert

# Unread alerts only
python main.py alert --unread

# Filter by severity
python main.py alert --severity high
python main.py alert --severity medium

# Filter by platform
python main.py alert --platform nitter

# Acknowledge an alert by ID
python main.py alert --ack 42
```

Alert severity is color-coded: **RED** = high, **YELLOW** = medium, **GREEN** = low.
Acknowledged alerts are shown in dim text.

---

### `report`

Generate intelligence reports.

```bash
# Rich tree in terminal (default): Campaign → Accounts → Posts
python main.py report

# Export to JSON file in data/
python main.py report --format json

# Export to CSV files in data/
python main.py report --format csv

# Filter
python main.py report --keyword "replacement" --time-window 48h
python main.py report --platform 4chan --format json
```

---

### `status`

System dashboard: post counts, flagged accounts, active campaigns, unread alerts, session stats.

```bash
python main.py status
```

---

### `watch`

Real-time read-only dashboard. Refreshes every 60 seconds from the local DB.
**Does not trigger collection or analysis.**

```bash
python main.py watch
```

| Key | Action |
|---|---|
| `m` | Main view (counts + recent alerts) |
| `a` | Alerts view (unread alerts table) |
| `w` | Watchlist view |
| `q` | Quit |

---

### `watch-add`

Add an item to the watchlist.

```bash
python main.py watch-add <value> [--type keyword|account|hashtag] [--platform <platform>]

# Examples
python main.py watch-add "great replacement"
python main.py watch-add "#StopTheSteal"        --type hashtag
python main.py watch-add kremlinbot             --type account --platform nitter
python main.py watch-add "NATO propaganda"      --type keyword --platform telegram
```

---

### `export-state` / `import-state`

Persist and restore the full database and configuration.

```bash
# Export to current directory
python main.py export-state
# → threadhunt_state_20241215_143022.tar.gz

# Restore from archive
python main.py import-state threadhunt_state_20241215_143022.tar.gz
```

The archive contains `threadhunt.db` and `config.json`. Copy it to a USB drive or encrypted storage before powering off a Live USB session.

---

### `config`

View and set configuration values.

```bash
# Show all settings and API key status
python main.py config
python main.py config --show-keys

# Adjust thresholds
python main.py config --set-key bot_score_threshold      0.65
python main.py config --set-key campaign_min_accounts    5
python main.py config --set-key campaign_time_window_minutes 60
python main.py config --set-key collector_post_cap       200

# Enable Tor routing
python main.py config --set-key use_tor true

# Set API keys (see Optional API Keys section below)
python main.py config --set-key twitter_bearer_token  <token>
python main.py config --set-key openai_api_key        <key>
python main.py config --set-key shodan_api_key        <key>
```

| Key | Default | Description |
|---|---|---|
| `bot_score_threshold` | `0.7` | Score at which an account is flagged |
| `campaign_min_accounts` | `3` | Minimum unique accounts to trigger a campaign |
| `campaign_time_window_minutes` | `30` | Time window for campaign clustering |
| `campaign_simhash_distance` | `5` | Max Hamming distance for near-duplicate detection |
| `collector_post_cap` | `500` | Max posts per collection run |
| `fourchan_post_cap` | `1000` | Max 4chan posts per run |
| `watch_refresh_seconds` | `60` | Watch mode refresh interval |
| `use_tor` | `false` | Route all HTTP through Tor SOCKS5 |

---

## Bot Scoring

Scores range from 0.0 (organic) to 1.0 (almost certainly automated). Accounts above the threshold (default 0.7) are flagged.

| Factor | Weight | Signal |
|---|---|---|
| Username randomness | 20% | Digit suffix pattern, high character entropy |
| Follower/following ratio | 25% | High following count with few followers |
| Post frequency | 25% | Abnormal posts/hour rate |
| Profile image reuse | 15% | Identical image hash across multiple accounts |
| Content duplication | 15% | Near-duplicate posts shared with other accounts |

Bot score is displayed as a color-coded bar in all tables:

```
████████ 0.9   ← RED    (flagged, ≥ 0.7)
████░░░░ 0.5   ← YELLOW (suspicious, 0.4–0.7)
██░░░░░░ 0.2   ← GREEN  (likely organic, < 0.4)
```

---

## Alert Types

| Type | Trigger Condition | Default Severity |
|---|---|---|
| `coordinated_campaign` | ≥3 accounts post near-identical content within 30 min | Confidence-based |
| `bot_detected` | Account bot score crosses threshold | High (≥0.85) / Medium |
| `narrative_reemergence` | Watchlist keyword silent for 48h then reappears | Medium |
| `keyword_spike` | Keyword volume 3× above 6-hour baseline in last 30 min | Medium |

---

## Optional API Keys

All API modules are fully implemented but remain dormant until a key is configured. The tool runs at full capability without any of them.

### Twitter v2 (deeper historical data)

```bash
python main.py config --set-key twitter_bearer_token <your_bearer_token>
python main.py collect --platform nitter --target <username>   # now uses v2 API
```

Provides: full historical timeline, engagement metrics, higher rate limits.

### OpenAI (narrative summaries in reports)

```bash
python main.py config --set-key openai_api_key <your_api_key>
python main.py report --format json   # reports now include GPT summaries
```

Provides: campaign narrative summaries, rhetorical register classification, translation fallback.

### Shodan (infrastructure attribution)

```bash
python main.py config --set-key shodan_api_key <your_api_key>
```

Provides: IP/ASN lookup for attribution, co-hosted domain discovery, infrastructure pivoting from identified bot networks.

---

## Tor Routing

All HTTP requests can be routed through Tor. Kali Linux ships with Tor.

```bash
sudo service tor start
python main.py config --set-key use_tor true
python main.py status   # confirm Tor is active in logs
```

Requires `tor` service running on `127.0.0.1:9050` (default). Falls back to direct connection if Tor is unreachable and logs a warning.

---

## Performance Notes for Live USB

THREADHUNT was designed specifically for Kali Linux Live USB sessions with 2–4 GB RAM and slow USB disk I/O.

- **Stream processing only** — no full table loads. All DB reads use `cursor.fetchmany(500)`.
- **SQLite WAL mode** — concurrent reads don't block writes. Reduced fsync pressure for slow USB storage.
- **No pandas / numpy / NetworkX** — the entire analysis stack runs in pure Python with stdlib only (plus the five listed packages).
- **Generators everywhere** — iterative processing avoids building large in-memory collections.
- **Bounded queries** — all table scans are capped. Campaign detection limits to 10,000 posts per window. Bot scoring duplication check limits to 2,000 comparison rows.
- **State export before shutdown** — `export-state` packs DB + config to a single `.tar.gz` in seconds. Copy to persistent storage.

---

## Project Structure

```
threadhunt/
├── main.py                      # CLI entry point, all commands, watch mode
├── config.py                    # Config singleton, API key management
├── db.py                        # SQLite schema, stream helpers, write helpers
├── requirements.txt
├── install.sh
├── collectors/
│   ├── nitter.py                # Twitter/X via public Nitter instances
│   ├── nitter_playwright.py     # Optional: headless Firefox for JS-protected instances
│   ├── fourchan.py              # 4chan JSON API (/pol/, /k/, /int/)
│   ├── telegram.py              # Telegram public channels (t.me/s/)
│   ├── vk.py                    # VK public walls
│   ├── web.py                   # Generic scraper + Wayback Machine
│   └── with_api/
│       ├── twitter_v2.py        # Twitter v2 API (requires bearer token)
│       ├── openai_nlp.py        # GPT summaries (requires API key)
│       └── shodan.py            # Infrastructure mapping (requires API key)
├── analysis/
│   ├── bot_score.py             # Heuristic bot scoring
│   ├── campaign_engine.py       # SimHash clustering, campaign detection
│   ├── identity_linker.py       # Cross-platform identity linking
│   ├── temporal.py              # Posting patterns, burst detection
│   └── similarity.py            # Language detection, translation
├── alerts/
│   └── triggers.py              # Four alert trigger conditions
├── reports/
│   ├── json_export.py
│   └── csv_export.py
└── utils/
    ├── hashing.py               # 64-bit SimHash from scratch, Hamming distance
    ├── text.py                  # Tokenization, username analysis, extraction
    └── tor.py                   # Tor SOCKS5 session helper
```

---

## Collector Notes

| Collector | Notes |
|---|---|
| **Nitter** | Health-checks 8 public instances on startup. Rotates automatically on failure. Paginates via cursor. Falls back through: requests timeline → requests search (`from:`) → Playwright timeline → Playwright search → Twitter guest API → Twitter CDN syndication. Playwright paths activate automatically if installed. |
| **4chan** | Uses the official free JSON API. Polls catalog, matches threads against watchlist, archives matching threads in full. |
| **Telegram** | Scrapes `t.me/s/{channel}` public preview. No Telethon, no API key. Limited to publicly visible messages. |
| **VK** | Scrapes initial server-side HTML render (~15–30 posts visible without JS). Deep historical collection requires the VK API. |
| **Web** | Scrapes any public URL, extracts main text content, attempts Wayback Machine snapshot as evidence preservation. |

---

## License

MIT License — see [LICENSE](LICENSE) for full text.

```
Copyright (c) 2024 GlobalReconReport

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

---

## Disclaimer

THREADHUNT is designed for authorized intelligence research, counter-extremism analysis, and disinformation investigation against **public** data sources only. Use responsibly and in accordance with applicable laws and the terms of service of any platform you collect from.
