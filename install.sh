#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# THREADHUNT — install.sh
# Checks Python version, installs dependencies, initializes database.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Terminal colors ───────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
DIM='\033[2m'
BOLD='\033[1m'
RESET='\033[0m'

# ── Change to script directory ────────────────────────────────────────────────
cd "$(dirname "$(realpath "$0")")"

# ── Banner ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${RED} _____ _                        _   _   _             _   ${RESET}"
echo -e "${RED}|_   _| |__  _ __ ___  __ _  __| | | | | |_   _ _ __ | |_ ${RESET}"
echo -e "${RED}  | | | '_ \\| '__/ _ \\/ _\` |/ _\` | | |_| | | | | '_ \\| __|${RESET}"
echo -e "${RED}  | | | | | | | |  __/ (_| | (_| | |  _  | |_| | | | | |_ ${RESET}"
echo -e "${RED}  |_| |_| |_|_|  \\___|\\__,_|\\__,_| |_| |_|\\__,_|_| |_|\\__|${RESET}"
echo -e "${DIM}  [ COORDINATED THREAT DETECTION ]  [ OSINT PLATFORM ]  [ FIELD EDITION ]${RESET}"
echo ""
echo -e "${BOLD}THREADHUNT Installer${RESET}"
echo -e "${DIM}────────────────────────────────────────────────────────────${RESET}"
echo ""

# ── Step 1: Check Python version ─────────────────────────────────────────────
echo -e "${CYAN}[1/5]${RESET} Checking Python version..."

PYTHON_CMD=""
for cmd in python3.12 python3.11 python3.10 python3; do
    if command -v "$cmd" &>/dev/null; then
        VERSION=$("$cmd" -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')
        MAJOR=$(echo "$VERSION" | cut -d. -f1)
        MINOR=$(echo "$VERSION" | cut -d. -f2)
        if [ "$MAJOR" -ge 3 ] && [ "$MINOR" -ge 10 ]; then
            PYTHON_CMD="$cmd"
            break
        fi
    fi
done

if [ -z "$PYTHON_CMD" ]; then
    echo -e "${RED}  ✗ Python 3.10+ not found.${RESET}"
    echo -e "${DIM}  Install with: sudo apt install python3 python3-pip python3-venv${RESET}"
    exit 1
fi

echo -e "${GREEN}  ✓ Found ${PYTHON_CMD} ($VERSION)${RESET}"

# ── Step 2: Virtual environment (optional, recommended) ──────────────────────
echo ""
echo -e "${CYAN}[2/5]${RESET} Setting up Python environment..."

USE_VENV=false

# Auto-detect if we're already inside a venv
if [ -n "${VIRTUAL_ENV:-}" ]; then
    echo -e "${GREEN}  ✓ Already inside a virtual environment: ${VIRTUAL_ENV}${RESET}"
    PIP_CMD="pip"
    PYTHON_RUN="$PYTHON_CMD"
    USE_VENV=true
elif [ -d ".venv" ]; then
    echo -e "${YELLOW}  ⚑ Found existing .venv — activating${RESET}"
    source .venv/bin/activate
    PIP_CMD="pip"
    PYTHON_RUN="python"
    USE_VENV=true
else
    # Offer to create venv, but don't fail if user declines
    echo -e "${DIM}  Virtual environment not detected.${RESET}"
    read -r -p "  Create a virtual environment in .venv/? [Y/n] " REPLY
    REPLY="${REPLY:-Y}"
    if [[ "$REPLY" =~ ^[Yy]$ ]]; then
        "$PYTHON_CMD" -m venv .venv
        source .venv/bin/activate
        PIP_CMD="pip"
        PYTHON_RUN="python"
        USE_VENV=true
        echo -e "${GREEN}  ✓ Virtual environment created and activated${RESET}"
    else
        echo -e "${YELLOW}  ⚑ Skipping venv — installing system-wide${RESET}"
        PIP_CMD="pip3"
        PYTHON_RUN="$PYTHON_CMD"
    fi
fi

# ── Step 3: Install dependencies ─────────────────────────────────────────────
echo ""
echo -e "${CYAN}[3/5]${RESET} Installing dependencies..."

# Detect if we need --break-system-packages (PEP 668 systems like Kali 2024+)
INSTALL_FLAGS=""
if ! $USE_VENV; then
    if $PIP_CMD install --dry-run rich 2>&1 | grep -q "externally-managed"; then
        INSTALL_FLAGS="--break-system-packages"
        echo -e "${YELLOW}  ⚑ PEP 668 system detected — using --break-system-packages${RESET}"
    fi
fi

$PIP_CMD install -r requirements.txt $INSTALL_FLAGS -q 2>&1 | grep -v "^$" | \
    grep -v "already satisfied" | \
    sed "s/^/  /" || true

# Verify key packages imported correctly
$PYTHON_RUN -c "import rich, requests, bs4, langdetect, Levenshtein" 2>/dev/null \
    && echo -e "${GREEN}  ✓ All packages installed successfully${RESET}" \
    || { echo -e "${RED}  ✗ Package import check failed — check errors above${RESET}"; exit 1; }

# ── Step 4: Playwright (auto-install) ────────────────────────────────────────
echo ""
echo -e "${CYAN}[4/5]${RESET} Installing Playwright (Nitter/Twitter JS-protected fallback)..."
echo -e "  ${DIM}Headless Firefox reaches Cloudflare-fronted instances (e.g. nitter.net).${RESET}"
echo -e "  ${DIM}Requires ~200 MB for the Firefox browser binary.${RESET}"

# Skip the pip install if playwright is already importable.
if $PYTHON_RUN -c "import playwright" 2>/dev/null; then
    echo -e "${GREEN}  ✓ Playwright Python package already installed${RESET}"
else
    echo -e "  Installing playwright Python package..."
    $PIP_CMD install playwright $INSTALL_FLAGS -q 2>&1 | grep -v "^$" | \
        grep -v "already satisfied" | sed "s/^/  /" || true
fi

echo -e "  Installing Firefox browser binary..."
# Try with sudo if plain install fails (Playwright needs write access to its browser dir).
# `playwright install firefox` is itself idempotent — re-running it is a no-op when present.
if ! $PYTHON_RUN -m playwright install firefox 2>/dev/null; then
    if command -v sudo &>/dev/null && sudo $PYTHON_RUN -m playwright install firefox 2>/dev/null; then
        echo -e "${GREEN}  ✓ Playwright + Firefox installed (sudo)${RESET}"
    else
        echo -e "${YELLOW}  ⚑ Playwright Python package installed but Firefox binary failed.${RESET}"
        echo -e "${DIM}    Run manually: sudo $PYTHON_RUN -m playwright install firefox${RESET}"
    fi
else
    echo -e "${GREEN}  ✓ Playwright + Firefox installed${RESET}"
fi

# ── Step 5: Initialize database ──────────────────────────────────────────────
echo ""
echo -e "${CYAN}[5/5]${RESET} Initializing database..."

chmod +x main.py
$PYTHON_RUN main.py db-init 2>&1 | grep -v "banner\|╭\|│\|╰" | sed "s/^/  /" || true
echo -e "${GREEN}  ✓ Database ready${RESET}"

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${DIM}────────────────────────────────────────────────────────────${RESET}"
echo -e "${GREEN}${BOLD}  THREADHUNT installed successfully.${RESET}"
echo ""

if $USE_VENV && [ -z "${VIRTUAL_ENV:-}" ]; then
    echo -e "${YELLOW}  Activate your environment before use:${RESET}"
    echo -e "  ${DIM}source .venv/bin/activate${RESET}"
    echo ""
fi

echo -e "${BOLD}  First steps:${RESET}"
echo -e "  ${CYAN}python main.py watch-add \"your keyword\"${RESET}   # seed watchlist"
echo -e "  ${CYAN}python main.py collect --platform 4chan --target pol${RESET}"
echo -e "  ${CYAN}python main.py analyze${RESET}"
echo -e "  ${CYAN}python main.py alert --unread${RESET}"
echo -e "  ${CYAN}python main.py status${RESET}"
echo ""
echo -e "  Full documentation: ${DIM}README.md${RESET}"
echo -e "${DIM}────────────────────────────────────────────────────────────${RESET}"
echo ""
