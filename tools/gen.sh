#!/bin/bash
# Run INSIDE WSL. Make another dataset for differential testing: tools/gen.sh SEED EVENTS [DIR]
# Default DIR: ~/ssdata/s<SEED>-<EVENTS>. Clicks = EVENTS/4.
set -e; s=$1; e=$2; d=${3:-$HOME/ssdata/s$s-$e}
[ -d "$d" ] || generate --seed "$s" --events "$e" --clicks $((e/4)) --output "$d"; echo "$d"
