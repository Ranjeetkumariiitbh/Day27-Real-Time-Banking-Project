#!/usr/bin/env bash
set -e

echo "Starting TCP relay on port 9999."
python3 scripts/tcp_relay.py
