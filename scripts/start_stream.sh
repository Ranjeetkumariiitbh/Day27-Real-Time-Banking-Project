#!/usr/bin/env bash
set -e

echo "Starting Day27 banking streaming application..."
sbt "run localhost 9999 data/branches.csv data/risk_accounts.csv"
