#!/bin/sh
# Build without Docker, run the 36 fixtures, and diff the dev set if it is here.
# Needs Rust and `pip install polars`. Dev set: `make dev` (needs `generate`)
# and dev-answer.zip unzipped into dev-answer/.
set -e
cd "$(dirname "$0")/.."
cargo build --release
BIN=./target/x86_64-unknown-linux-gnu/release/shopping-sessions
python3 scripts/run_fixtures.py fixtures -- "$BIN" | tail -1
if [ -d dev ] && [ -d dev-answer ]; then
  OUT=$(mktemp -d)
  start=$(date +%s.%N)
  DATA_DIR=dev OUTPUT_DIR="$OUT" "$BIN"
  end=$(date +%s.%N)
  echo "dev run: $(echo "$end - $start" | bc) s"
  python3 scripts/compare.py "$OUT" dev-answer | tail -1
fi
