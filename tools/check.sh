#!/bin/bash
# Run INSIDE WSL (use tools/wsl.sh from Windows). Runs a pipeline natively and checks it.
#   tools/check.sh fixtures -- CMD...   all 36 edge-case fixtures (CMD gets DATA_DIR/OUTPUT_DIR env + last two args)
#   tools/check.sh dev      -- CMD...   1M dev set (~/ssdata/dev), exact diff vs dev-answer/, prints wall time
#   tools/check.sh bench    -- CMD...   timed run on ~/ssdata/b5m (5M events), serialized machine-wide by a lock
#   tools/check.sh all      -- CMD...   fixtures, dev, bench
# Outputs go to ~/ssout/<name>; compare two outputs with: python scripts/compare.py A B
set -euo pipefail
mode=$1; shift; [ "$1" = "--" ] && shift
repo=$(cd "$(dirname "$0")/.." && pwd)
export PATH=$HOME/ssenv/bin:$HOME/.local/bin:$PATH
NCPU=${NCPU:-$(nproc)}; MEM_GB=${MEM_GB:-3}; export NCPU MEM_GB
tag=$(basename "$repo")-$$
timed() { # data out -> runs CMD, prints seconds
  local data=$1 out=$2 scratch=$HOME/ssscratch/$tag; shift 2
  rm -rf "$out" "$scratch"; mkdir -p "$out" "$scratch"
  local t0=$(date +%s.%N)
  DATA_DIR=$data OUTPUT_DIR=$out SCRATCH_DIR=$scratch "$@" "$data" "$out"
  local rc=$?; local t1=$(date +%s.%N); rm -rf "$scratch"
  echo "WALL $(echo "$t1 - $t0" | bc) s (exit $rc)"; return $rc
}
case $mode in
  fixtures) cd "$repo" && python scripts/run_fixtures.py fixtures -- "$@" ;;
  dev) timed "$HOME/ssdata/dev" "$HOME/ssout/$tag-dev" "$@" && python "$repo/scripts/compare.py" "$HOME/ssout/$tag-dev" "$repo/dev-answer" ;;
  bench) exec 9>"$HOME/ssdata/.bench.lock"; echo "waiting for bench lock..."; flock 9
         timed "$HOME/ssdata/b5m" "$HOME/ssout/$tag-b5m" "$@" ;;
  all) "$0" fixtures -- "$@" && "$0" dev -- "$@" && "$0" bench -- "$@" ;;
  *) echo "unknown mode $mode"; exit 2 ;;
esac
