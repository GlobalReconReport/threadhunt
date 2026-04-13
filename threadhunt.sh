#!/bin/bash
# Wrapper — run from anywhere: ./threadhunt.sh collect --platform nitter ...
cd "$(dirname "$(realpath "$0")")"
python3 main.py "$@"
