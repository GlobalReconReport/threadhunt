#!/usr/bin/env python3
"""
THREADHUNT — Field-deployable OSINT intelligence terminal.
Coordinated threat detection across public social media platforms.

Usage:  python main.py <command> [options]
        ./threadhunt.sh <command> [options]
"""
import sys
import os
import argparse
import logging
import json
import time
import tarfile
import shutil
from datetime import datetime, timezone
from pathlib import Path

# ── Ensure project root is importable regardless of CWD ──────────────────────
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree
from rich.text import Text
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.live import Live
from rich.markup import escape

import config as cfg
import db

console = Console()

# ── Logging setup ─────────────────────────────────────────────────────────────

LOG_DIR = Path(__file__).parent / 'logs'
LOG_DIR.mkdir(exist_ok=True)

_log_handler_file = logging.FileHandler(str(LOG_DIR / 'threadhunt.log'))
_log_handler_file.setFormatter(
    logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
)
logging.getLogger('threadhunt').addHandler(_log_handler_file)
logging.getLogger('threadhunt').setLevel(logging.DEBUG)

# Suppress noisy third-party loggers
for lib in ('urllib3', 'requests', 'charset_normalizer'):
    logging.getLogger(lib).setLevel(logging.WARNING)

logger = logging.getLogger('threadhunt')


# ── Banner ────────────────────────────────────────────────────────────────────

BANNER_ART = r"""[bold red] _____ _                        _   _   _             _
|_   _| |__  _ __ ___  __ _  __| | | | | |_   _ _ __ | |_
  | | | '_ \| '__/ _ \/ _` |/ _` | | |_| | | | | '_ \| __|
  | | | | | | | |  __/ (_| | (_| | |  _  | |_| | | | | |_
  |_| |_| |_|_|  \___|\__,_|\__,_| |_| |_|\__,_|_| |_|\__|[/bold red]

[dim white]  [ COORDINATED THREAT DETECTION ]  [ OSINT PLATFORM ]  [ FIELD EDITION ][/dim white]"""


def show_banner():
    console.print(Panel(BANNER_ART, border_style="dim red", padding=(0, 2)))


# ── Visual helpers ────────────────────────────────────────────────────────────

def bot_score_bar(score: float) -> str:
    """Render score as colored block bar: ████░░░░ 0.7"""
    filled = int(round(score * 8))
    bar    = '█' * filled + '░' * (8 - filled)
    s      = f"{score:.1f}"
    if score >= 0.7:   return f"[red]{bar} {s}[/red]"
    if score >= 0.4:   return f"[yellow]{bar} {s}[/yellow]"
    return f"[green]{bar} {s}[/green]"


def severity_color(severity: str) -> str:
    return {'high': 'red', 'medium': 'yellow', 'low': 'green'}.get(severity.lower(), 'white')


def platform_tag(platform: str) -> str:
    return f"[cyan]{escape(platform)}[/cyan]"


def _get_session(use_tor: bool = None):
    from utils.tor import get_session
    tor = use_tor if use_tor is not None else cfg.get('use_tor', False)
    return get_session(use_tor=tor)


# ═══════════════════════════════════════════════════════════════════════════════
# COMMANDS
# ═══════════════════════════════════════════════════════════════════════════════

# ── db-init ───────────────────────────────────────────────────────────────────

def cmd_db_init(args):
    db.init_db()
    console.print("[green]Database initialized.[/green]")
    console.print(f"[dim]Location: {db._get_db_path()}[/dim]")


# ── collect ───────────────────────────────────────────────────────────────────

def cmd_collect(args):
    platform = args.platform.lower()
    target   = args.target
    keyword  = getattr(args, 'keyword', None)
    verbose  = cfg.get('verbose', False) or getattr(args, 'verbose', False)

    session = _get_session()

    COLLECTORS = {
        'nitter':   'collectors.nitter',
        'twitter':  'collectors.nitter',
        '4chan':     'collectors.fourchan',
        'fourchan':  'collectors.fourchan',
        'telegram':  'collectors.telegram',
        'vk':        'collectors.vk',
        'web':       'collectors.web',
    }

    if platform not in COLLECTORS:
        console.print(f"[red]Unknown platform: {platform}[/red]")
        console.print(f"[dim]Supported: {', '.join(sorted(set(COLLECTORS.keys())))}[/dim]")
        sys.exit(1)

    import importlib
    mod = importlib.import_module(COLLECTORS[platform])

    # Resolve target list — platforms that support bulk collection without --target
    if target:
        targets = [target]
    elif platform in ('telegram',):
        targets = cfg.get('telegram_channels', [])
        if not targets:
            console.print("[yellow]No telegram_channels configured. Use --target or set telegram_channels in config.[/yellow]")
            return
        console.print(f"[dim]Collecting from {len(targets)} configured Telegram channels[/dim]")
    elif platform in ('vk',):
        targets = cfg.get('vk_groups', [])
        if not targets:
            console.print("[yellow]No vk_groups configured. Use --target or set vk_groups in config.[/yellow]")
            return
        console.print(f"[dim]Collecting from {len(targets)} configured VK groups[/dim]")
    elif platform in ('4chan', 'fourchan'):
        # fourchan collector handles None target internally (uses fourchan_boards)
        targets = [None]
    else:
        console.print(f"[red]--target required for platform: {platform}[/red]")
        return

    # Session tracking
    with db.get_conn() as conn:
        session_id = db.start_session(conn, platform=platform)

    new_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task(f"[cyan]{platform}[/cyan]", total=None)

        def verbose_cb(msg: str):
            if verbose:
                progress.update(task, description=f"[cyan]{platform}[/cyan] {escape(str(msg))}")

        for t in targets:
            label = t or platform
            progress.update(task, description=f"[cyan]{platform}[/cyan] → {escape(str(label))}")
            try:
                n = mod.collect(session, t, keyword=keyword, verbose_cb=verbose_cb)
                new_count += n
                logger.info("Collect: %d posts from %s/%s", n, platform, label)
            except Exception as e:
                logger.error("Collector error [%s/%s]: %s", platform, label, e)
                console.print(f"[red]Collector error [{label}]: {e}[/red]")

    # Update session
    with db.get_conn() as conn:
        db.end_session(conn, session_id,
                       posts_collected=new_count, alerts_triggered=0)

    target_label = target or f"({len(targets)} targets)"
    console.print(
        f"[green]✓[/green] Collected [bold]{new_count}[/bold] new posts "
        f"from [cyan]{platform}[/cyan] / {escape(str(target_label))}"
    )


# ── analyze ───────────────────────────────────────────────────────────────────

def cmd_analyze(args):
    console.print("[dim]Running analysis pipeline...[/dim]\n")

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        console=console,
    ) as progress:

        # Step 1: Bot scoring
        task1 = progress.add_task("[cyan]Bot scoring...", total=100)
        from analysis.bot_score import score_all_accounts

        def _bot_progress(current, total):
            if total > 0:
                progress.update(task1, completed=int(current / total * 100))

        flagged = score_all_accounts(progress_cb=_bot_progress)
        progress.update(task1, completed=100,
                        description=f"[green]Bot scoring complete — {flagged} flagged")

        # Step 2: Campaign detection
        task2 = progress.add_task("[cyan]Campaign detection...", total=100)
        from analysis import campaign_engine

        def _camp_progress(current, total):
            if total > 0:
                progress.update(task2, completed=int(current / total * 100))

        new_campaigns = campaign_engine.run(progress_cb=_camp_progress)
        progress.update(task2, completed=100,
                        description=f"[green]Campaigns — {new_campaigns} new detected")

        # Step 3: Language tagging
        task3 = progress.add_task("[cyan]Language tagging...", total=None)
        from analysis.similarity import tag_posts_language
        with db.get_conn() as conn:
            lang_tagged = tag_posts_language(conn)
        progress.update(task3, description=f"[green]Language — {lang_tagged} posts tagged",
                        total=1, completed=1)

        # Step 4: Temporal analysis — posting entropy + timezone inference
        task_temporal = progress.add_task("[cyan]Temporal profiling...", total=None)
        from analysis.temporal import run_temporal_analysis
        with db.get_conn() as conn:
            temporal_count = run_temporal_analysis(conn)
        progress.update(task_temporal,
                        description=f"[green]Temporal — {temporal_count} accounts profiled",
                        total=1, completed=1)

        # Step 5: Identity linking — cross-platform username/pic/timing signals
        task_identity = progress.add_task("[cyan]Identity linking...", total=None)
        from analysis.identity_linker import run_identity_linking
        with db.get_conn() as conn:
            id_report = run_identity_linking(conn)
        id_links = len(id_report.get('username_links', []))
        id_corr  = len(id_report.get('time_correlated', []))
        progress.update(task_identity,
                        description=f"[green]Identity — {id_links} username links, {id_corr} time-correlated",
                        total=1, completed=1)

        # Step 6: Alert triggers
        task4 = progress.add_task("[cyan]Checking alert triggers...", total=None)
        from alerts.triggers import check_all
        with db.get_conn() as conn:
            new_alerts = check_all(conn)
        progress.update(task4,
                        description=f"[green]Alerts — {new_alerts} new triggered",
                        total=1, completed=1)

    console.print(f"\n[green]Analysis complete.[/green]")
    console.print(f"  Flagged bots:      [red]{flagged}[/red]")
    console.print(f"  New campaigns:     [yellow]{new_campaigns}[/yellow]")
    console.print(f"  Temporal profiled: [cyan]{temporal_count}[/cyan]")
    console.print(f"  Identity links:    [cyan]{id_links}[/cyan]  Time-correlated: [cyan]{id_corr}[/cyan]")
    console.print(f"  New alerts:        [red]{new_alerts}[/red]")


# ── alert ─────────────────────────────────────────────────────────────────────

def cmd_alert(args):
    unread_only = getattr(args, 'unread', False)
    ack_id      = getattr(args, 'ack', None)
    platform    = getattr(args, 'platform', None)
    severity    = getattr(args, 'severity', None)

    if ack_id is not None:
        with db.get_conn() as conn:
            conn.execute("UPDATE alerts SET acknowledged=1 WHERE id=?", (ack_id,))
        console.print(f"[green]Alert #{ack_id} acknowledged.[/green]")
        return

    where = []
    params = []

    if unread_only:
        where.append("acknowledged=0")
    if platform:
        where.append("platform=?")
        params.append(platform)
    if severity:
        where.append("severity=?")
        params.append(severity.lower())

    where_str = ' AND '.join(where) if where else '1=1'

    t = Table(title="Alerts", show_header=True, header_style="bold",
              border_style="dim")
    t.add_column("ID",       style="dim",    width=5)
    t.add_column("Type",     width=24)
    t.add_column("Severity", width=8)
    t.add_column("Platform", style="cyan",   width=14)
    t.add_column("Keyword",  width=16)
    t.add_column("Time",     style="dim",    width=20)
    t.add_column("Ack",      width=4)

    row_count = 0
    with db.get_conn() as conn:
        for row in db.stream_rows(conn, f"""
            SELECT id, alert_type, severity, platform, keyword,
                   created_at, acknowledged, description
            FROM alerts WHERE {where_str}
            ORDER BY created_at DESC LIMIT 100
        """, tuple(params)):
            is_ack = row['acknowledged']
            sev    = row['severity'] or 'low'
            color  = severity_color(sev)
            style  = "dim" if is_ack else ""

            t.add_row(
                f"[{style}]{row['id']}[/{style}]" if style else str(row['id']),
                f"[{style}]{escape(row['alert_type'] or '')}[/{style}]" if style else escape(row['alert_type'] or ''),
                f"[{color}]{sev.upper()}[/{color}]",
                platform_tag(row['platform'] or '-'),
                f"[dim]{escape(row['keyword'] or '-')}[/dim]",
                f"[dim]{(row['created_at'] or '')[:19]}[/dim]",
                "[green]✓[/green]" if is_ack else "[dim]·[/dim]",
            )
            row_count += 1

    if row_count == 0:
        console.print("[dim]No alerts.[/dim]")
    else:
        console.print(t)
        console.print(f"[dim]Use --ack <id> to acknowledge. --unread for unread only.[/dim]")


# ── report ────────────────────────────────────────────────────────────────────

def cmd_report(args):
    fmt         = getattr(args, 'format', 'json') or 'json'
    platform    = getattr(args, 'platform', None)
    keyword     = getattr(args, 'keyword', None)
    time_window = getattr(args, 'time_window', None)

    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')

    if fmt == 'json':
        from reports.json_export import export
        out = str(cfg.DATA_DIR / f'report_{ts}.json')
        count = export(out, platform=platform, keyword=keyword, time_window=time_window)
        console.print(f"[green]JSON report:[/green] {out}  ({count} campaigns)")

    elif fmt == 'csv':
        from reports.csv_export import export
        out = str(cfg.DATA_DIR / f'report_{ts}')
        counts = export(out, platform=platform, keyword=keyword, time_window=time_window)
        console.print(f"[green]CSV reports written to:[/green] {out}/")
        for name, count in counts.items():
            console.print(f"  [cyan]{name}[/cyan]: {count} rows")

    else:
        # Rich Tree format: Campaign → Cluster → Accounts → Posts
        _print_tree_report(platform, keyword, time_window)


def _print_tree_report(platform, keyword, time_window):
    from analysis.campaign_engine import get_active_campaigns, get_campaign_posts

    with db.get_conn() as conn:
        campaigns = get_active_campaigns(conn)

        if not campaigns:
            console.print("[dim]No active campaigns.[/dim]")
            return

        root = Tree("[bold]Active Campaigns[/bold]")

        shown = 0
        for camp in campaigns[:20]:
            if platform and camp.get('platform') != platform:
                continue
            if keyword and keyword.lower() not in (camp.get('keyword') or '').lower():
                continue

            conf = camp.get('confidence_score', 0)
            conf_color = 'red' if conf >= 0.7 else ('yellow' if conf >= 0.4 else 'green')
            camp_label = (
                f"[{conf_color}]Campaign #{camp['id']}[/{conf_color}] "
                f"[cyan]{escape(camp['platform'] or '')}[/cyan] "
                f"kw=[white]{escape(camp['keyword'] or '')}[/white] "
                f"conf=[{conf_color}]{conf:.2f}[/{conf_color}] "
                f"accts=[bold]{camp['account_count']}[/bold]"
            )
            camp_node = root.add(camp_label)

            posts = get_campaign_posts(camp['id'], conn, limit=10)
            if not posts:
                camp_node.add("[dim]no posts[/dim]")
                continue

            # Group posts by account
            by_account: dict = {}
            for p in posts:
                uname = p.get('username', 'unknown')
                by_account.setdefault(uname, []).append(p)

            for uname, uposts in list(by_account.items())[:5]:
                bscore = uposts[0].get('bot_score', 0.0) if uposts else 0.0
                acct_node = camp_node.add(
                    f"[white]@{escape(uname)}[/white] {bot_score_bar(bscore)}"
                )
                for p in uposts[:3]:
                    content_preview = (p.get('content') or '')[:80].replace('\n', ' ')
                    acct_node.add(
                        f"[dim]{escape(p.get('timestamp', '')[:16])}[/dim] "
                        f"{escape(content_preview)}…"
                    )

            shown += 1

        console.print(root)


# ── status ────────────────────────────────────────────────────────────────────

def cmd_status(args):
    with db.get_conn() as conn:
        total_posts      = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
        total_accounts   = conn.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
        flagged_accounts = conn.execute("SELECT COUNT(*) FROM accounts WHERE flagged=1").fetchone()[0]
        active_campaigns = conn.execute("SELECT COUNT(*) FROM campaigns WHERE active=1").fetchone()[0]
        unread_alerts    = conn.execute("SELECT COUNT(*) FROM alerts WHERE acknowledged=0").fetchone()[0]
        total_alerts     = conn.execute("SELECT COUNT(*) FROM alerts").fetchone()[0]
        watchlist_count  = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]

        # Platform breakdown
        platform_rows = conn.execute("""
            SELECT platform, COUNT(*) as cnt FROM posts
            GROUP BY platform ORDER BY cnt DESC LIMIT 10
        """).fetchall()

        # Last collector run per platform (latest post collected_at)
        last_runs = conn.execute("""
            SELECT platform, MAX(collected_at) FROM posts
            GROUP BY platform ORDER BY MAX(collected_at) DESC LIMIT 10
        """).fetchall()

        # Session stats (current/latest session)
        sess = conn.execute("""
            SELECT posts_collected, alerts_triggered, started_at
            FROM sessions ORDER BY id DESC LIMIT 1
        """).fetchone()

    status_content = (
        f"[white]Total posts:[/white]      [bold cyan]{total_posts:,}[/bold cyan]\n"
        f"[white]Accounts tracked:[/white] [bold]{total_accounts:,}[/bold]"
        f"  ([red]{flagged_accounts}[/red] flagged)\n"
        f"[white]Active campaigns:[/white] [bold yellow]{active_campaigns}[/bold yellow]\n"
        f"[white]Unread alerts:[/white]    [bold red]{unread_alerts}[/bold red]"
        f" / {total_alerts} total\n"
        f"[white]Watchlist items:[/white]  [cyan]{watchlist_count}[/cyan]"
    )
    if sess:
        status_content += (
            f"\n\n[dim]Last session:[/dim]  "
            f"[dim]{sess[2][:19] if sess[2] else 'n/a'}[/dim]\n"
            f"[dim]Posts collected:[/dim] [dim]{sess[0]}[/dim]  "
            f"[dim]Alerts triggered:[/dim] [dim]{sess[1]}[/dim]"
        )

    console.print(Panel(status_content, title="[bold]THREADHUNT STATUS[/bold]",
                        border_style="dim"))

    if platform_rows:
        t = Table(title="Posts by Platform", show_header=True,
                  header_style="bold", border_style="dim")
        t.add_column("Platform", style="cyan")
        t.add_column("Posts", justify="right")
        t.add_column("Last collected", style="dim")

        last_run_map = {row[0]: row[1] for row in last_runs}
        for row in platform_rows:
            t.add_row(
                row[0],
                f"{row[1]:,}",
                (last_run_map.get(row[0]) or '')[:19],
            )
        console.print(t)


# ── watch ─────────────────────────────────────────────────────────────────────

def cmd_watch(args):
    """
    Real-time DB watch. Reads only. No collection or analysis.
    q=quit, a=alerts view, w=watchlist view, m=main view.
    """
    import select
    import tty
    import termios

    refresh_interval = cfg.get('watch_refresh_seconds', 60)
    view = 'main'

    def _read_key_nb():
        """Non-blocking single-key read."""
        if select.select([sys.stdin], [], [], 0)[0]:
            return sys.stdin.read(1)
        return None

    def _build_main():
        with db.get_conn() as conn:
            posts     = conn.execute("SELECT COUNT(*) FROM posts").fetchone()[0]
            campaigns = conn.execute("SELECT COUNT(*) FROM campaigns WHERE active=1").fetchone()[0]
            unread    = conn.execute("SELECT COUNT(*) FROM alerts WHERE acknowledged=0").fetchone()[0]
            wl        = conn.execute("SELECT COUNT(*) FROM watchlist").fetchone()[0]
            # Recent alerts
            recent_alerts = conn.execute("""
                SELECT alert_type, severity, keyword, created_at
                FROM alerts WHERE acknowledged=0
                ORDER BY created_at DESC LIMIT 5
            """).fetchall()

        content = (
            f"[white]Posts in DB:[/white]      [cyan]{posts:,}[/cyan]\n"
            f"[white]Active Campaigns:[/white] [yellow]{campaigns}[/yellow]\n"
            f"[white]Unread Alerts:[/white]    [red]{unread}[/red]\n"
            f"[white]Watchlist Items:[/white]  [cyan]{wl}[/cyan]\n"
        )
        if recent_alerts:
            content += "\n[dim]Recent unread alerts:[/dim]\n"
            for a in recent_alerts:
                sc = severity_color(a[1] or 'low')
                content += (
                    f"  [{sc}]{(a[1] or '').upper():6}[/{sc}] "
                    f"[white]{escape(a[0] or '')}[/white] "
                    f"[dim]{escape(a[2] or '')} — {(a[3] or '')[:16]}[/dim]\n"
                )

        now = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        return Panel(
            content,
            title=f"[bold red]THREADHUNT[/bold red] [dim]WATCH[/dim]  [dim]{now}[/dim]",
            subtitle="[dim]q=quit  a=alerts  w=watchlist  m=main[/dim]",
            border_style="dim red",
        )

    def _build_alerts():
        rows = []
        with db.get_conn() as conn:
            for row in db.stream_rows(conn, """
                SELECT id, alert_type, severity, platform, keyword, created_at
                FROM alerts WHERE acknowledged=0
                ORDER BY created_at DESC LIMIT 20
            """):
                rows.append(dict(row))

        t = Table(show_header=True, header_style="bold", border_style="dim")
        t.add_column("ID",   style="dim", width=5)
        t.add_column("Type", width=22)
        t.add_column("Sev",  width=7)
        t.add_column("Platform", style="cyan", width=14)
        t.add_column("Keyword",  width=16)
        t.add_column("Time",     style="dim",  width=17)

        for row in rows:
            sc = severity_color(row.get('severity') or 'low')
            t.add_row(
                str(row['id']),
                escape(row.get('alert_type') or ''),
                f"[{sc}]{(row.get('severity') or '').upper()}[/{sc}]",
                escape(row.get('platform') or '-'),
                escape(row.get('keyword')  or '-'),
                (row.get('created_at') or '')[:16],
            )

        now = datetime.now(timezone.utc).strftime('%H:%M:%S UTC')
        return Panel(t, title=f"[bold]UNREAD ALERTS[/bold]  [dim]{now}[/dim]",
                     subtitle="[dim]m=main  q=quit[/dim]", border_style="dim red")

    def _build_watchlist():
        rows = []
        with db.get_conn() as conn:
            for row in db.stream_rows(conn,
                "SELECT type, value, platform, added_at FROM watchlist LIMIT 30"
            ):
                rows.append(dict(row))

        t = Table(show_header=True, header_style="bold", border_style="dim")
        t.add_column("Type",     style="cyan", width=12)
        t.add_column("Value",    width=30)
        t.add_column("Platform", width=14)
        t.add_column("Added",    style="dim", width=20)

        for row in rows:
            t.add_row(
                escape(row.get('type') or ''),
                escape(row.get('value') or ''),
                escape(row.get('platform') or 'all'),
                (row.get('added_at') or '')[:16],
            )

        return Panel(t, title="[bold]WATCHLIST[/bold]",
                     subtitle="[dim]m=main  q=quit[/dim]", border_style="dim")

    VIEW_BUILDERS = {
        'main':      _build_main,
        'alerts':    _build_alerts,
        'watchlist': _build_watchlist,
    }

    console.print("[dim]Watch mode. Press [bold]q[/bold]=quit  "
                  "[bold]a[/bold]=alerts  [bold]w[/bold]=watchlist  "
                  "[bold]m[/bold]=main[/dim]")

    fd = sys.stdin.fileno()
    old_settings = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        with Live(VIEW_BUILDERS[view](), auto_refresh=False,
                  screen=False, console=console) as live:
            last_refresh = 0.0
            while True:
                now = time.time()
                key = _read_key_nb()

                if key == 'q':
                    break
                elif key == 'a':
                    view = 'alerts'
                elif key == 'w':
                    view = 'watchlist'
                elif key == 'm':
                    view = 'main'

                if now - last_refresh >= refresh_interval or key in ('a', 'w', 'm'):
                    try:
                        live.update(VIEW_BUILDERS[view]())
                        live.refresh()
                    except Exception as e:
                        logger.debug("Watch refresh error: %s", e)
                    last_refresh = now

                time.sleep(0.05)

    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)

    console.print("[dim]Watch mode exited.[/dim]")


# ── config ────────────────────────────────────────────────────────────────────

def cmd_config(args):
    set_key   = getattr(args, 'set_key', None)
    show_keys = getattr(args, 'show_keys', False)

    if set_key:
        if len(set_key) != 2:
            console.print("[red]Usage: --set-key <name> <value>[/red]")
            sys.exit(1)
        cfg.set_value(set_key[0], set_key[1])
        console.print(f"[green]Set:[/green] {set_key[0]} = {set_key[1]}")
        return

    if show_keys or not set_key:
        c = cfg.load_config()

        t = Table(title="Configuration", show_header=True, border_style="dim")
        t.add_column("Key",   style="cyan", width=36)
        t.add_column("Value", width=40)

        skip = {'api_keys', 'nitter_instances', 'fourchan_boards'}
        for k, v in c.items():
            if k not in skip:
                t.add_row(k, str(v))
        console.print(t)

        t2 = Table(title="API Keys", show_header=True, border_style="dim")
        t2.add_column("Key",    style="cyan", width=30)
        t2.add_column("Status", width=16)
        for k, v in c.get('api_keys', {}).items():
            status = "[green]SET[/green]" if v else "[dim]not configured[/dim]"
            t2.add_row(k, status)
        console.print(t2)


# ── watch-add / watchlist management ─────────────────────────────────────────

def cmd_watch_add(args):
    """Add item to watchlist."""
    item_type = (getattr(args, 'type', None) or 'keyword').lower()
    value     = args.target
    platform  = getattr(args, 'platform', None)

    now = datetime.now(timezone.utc).isoformat()
    with db.get_conn() as conn:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO watchlist (type, value, platform, added_at)
                VALUES (?, ?, ?, ?)
            """, (item_type, value, platform, now))
        except Exception as e:
            console.print(f"[red]Error: {e}[/red]")
            return
    console.print(f"[green]Watchlist:[/green] added [{item_type}] {value}")


# ── export-state ──────────────────────────────────────────────────────────────

def cmd_export_state(args):
    """
    Pack the full SQLite DB + config into a timestamped .tar.gz.
    Critical for Live USB sessions — RAM is wiped on reboot.
    """
    ts = datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')
    out_path = Path(f'threadhunt_state_{ts}.tar.gz')

    data_dir = cfg.DATA_DIR

    with tarfile.open(str(out_path), 'w:gz') as tar:
        for fname in ('threadhunt.db', 'config.json'):
            fpath = data_dir / fname
            if fpath.exists():
                tar.add(str(fpath), arcname=fname)

    size_mb = out_path.stat().st_size / 1024 / 1024
    console.print(
        f"[green]State exported:[/green] {out_path}  "
        f"[dim]({size_mb:.1f} MB)[/dim]"
    )
    logger.info("State exported to %s", out_path)


# ── import-state ──────────────────────────────────────────────────────────────

def cmd_import_state(args):
    """Restore DB + config from a previously exported .tar.gz."""
    archive_path = Path(args.file)
    if not archive_path.exists():
        console.print(f"[red]File not found: {archive_path}[/red]")
        sys.exit(1)

    data_dir = cfg.DATA_DIR
    data_dir.mkdir(parents=True, exist_ok=True)

    # Backup existing DB before overwriting
    existing_db = data_dir / 'threadhunt.db'
    if existing_db.exists():
        backup = data_dir / f'threadhunt.db.backup_{int(time.time())}'
        shutil.copy2(str(existing_db), str(backup))
        console.print(f"[dim]Existing DB backed up to {backup.name}[/dim]")

    with tarfile.open(str(archive_path), 'r:gz') as tar:
        tar.extractall(path=str(data_dir))

    console.print(f"[green]State restored from:[/green] {archive_path}")

    # Reload config
    import importlib
    importlib.reload(cfg)
    cfg._config = None
    cfg.load_config()

    # Show quick status
    cmd_status(args)


# ═══════════════════════════════════════════════════════════════════════════════
# CLI PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog='threadhunt',
        description='THREADHUNT — coordinated threat detection OSINT terminal',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('--verbose', action='store_true',
                        help='Show collector progress in terminal')

    sub = parser.add_subparsers(dest='command', metavar='<command>')

    # ── db-init
    sub.add_parser('db-init', help='Initialize SQLite database')

    # ── collect
    p_collect = sub.add_parser('collect', help='Collect posts from a platform')
    p_collect.add_argument('--platform', required=True,
                           help='nitter | 4chan | telegram | vk | web')
    p_collect.add_argument('--target',   required=True,
                           help='Username, board, channel, or URL')
    p_collect.add_argument('--keyword',  help='Filter to posts containing this keyword')
    p_collect.add_argument('--verbose',  action='store_true')

    # ── analyze
    p_analyze = sub.add_parser('analyze', help='Run full analysis pipeline')
    p_analyze.add_argument('--platform',    help='Limit to platform')
    p_analyze.add_argument('--time-window', help='e.g. 24h, 7d')

    # ── alert
    p_alert = sub.add_parser('alert', help='View or acknowledge alerts')
    p_alert.add_argument('--unread',   action='store_true', help='Show only unread')
    p_alert.add_argument('--ack',      type=int, metavar='ID', help='Acknowledge alert by ID')
    p_alert.add_argument('--platform', help='Filter by platform')
    p_alert.add_argument('--severity', help='Filter: high | medium | low')

    # ── report
    p_report = sub.add_parser('report', help='Generate report')
    p_report.add_argument('--format',      choices=['json', 'csv', 'tree'],
                          default='tree', help='Output format (default: tree)')
    p_report.add_argument('--platform',    help='Filter by platform')
    p_report.add_argument('--keyword',     help='Filter by keyword')
    p_report.add_argument('--time-window', help='e.g. 24h, 7d')

    # ── config
    p_config = sub.add_parser('config', help='View or set configuration')
    p_config.add_argument('--set-key',   nargs=2, metavar=('NAME', 'VALUE'),
                          help='Set a config key or API key')
    p_config.add_argument('--show-keys', action='store_true',
                          help='Show all config and API key status')

    # ── status
    sub.add_parser('status', help='Show DB and session statistics')

    # ── watch
    sub.add_parser('watch', help='Real-time DB watch mode (read-only)')

    # ── watch-add
    p_wa = sub.add_parser('watch-add', help='Add item to watchlist')
    p_wa.add_argument('target',     help='Keyword, username, or hashtag to watch')
    p_wa.add_argument('--type',     default='keyword',
                      help='keyword | account | hashtag (default: keyword)')
    p_wa.add_argument('--platform', help='Limit to a specific platform')

    # ── export-state
    sub.add_parser('export-state', help='Export DB + config to tar.gz')

    # ── import-state
    p_import = sub.add_parser('import-state', help='Restore from tar.gz')
    p_import.add_argument('file', help='Path to threadhunt_state_*.tar.gz')

    return parser


COMMAND_MAP = {
    'db-init':      cmd_db_init,
    'collect':      cmd_collect,
    'analyze':      cmd_analyze,
    'alert':        cmd_alert,
    'report':       cmd_report,
    'config':       cmd_config,
    'status':       cmd_status,
    'watch':        cmd_watch,
    'watch-add':    cmd_watch_add,
    'export-state': cmd_export_state,
    'import-state': cmd_import_state,
}


def main():
    show_banner()

    # Ensure data dir exists
    cfg.DATA_DIR.mkdir(parents=True, exist_ok=True)

    parser = build_parser()
    args   = parser.parse_args()

    # Global verbose flag
    if getattr(args, 'verbose', False):
        cfg.set_value('verbose', 'true')
        # Add stderr handler for verbose mode
        _sh = logging.StreamHandler(sys.stderr)
        _sh.setLevel(logging.INFO)
        logging.getLogger('threadhunt').addHandler(_sh)

    if not args.command:
        parser.print_help()
        sys.exit(0)

    handler = COMMAND_MAP.get(args.command)
    if not handler:
        console.print(f"[red]Unknown command: {args.command}[/red]")
        sys.exit(1)

    try:
        handler(args)
    except KeyboardInterrupt:
        console.print("\n[dim]Interrupted.[/dim]")
        sys.exit(0)
    except Exception as e:
        logger.exception("Unhandled error in command '%s'", args.command)
        console.print(f"\n[red]Error:[/red] {e}")
        console.print("[dim]Full trace in logs/threadhunt.log[/dim]")
        sys.exit(1)


if __name__ == '__main__':
    main()
