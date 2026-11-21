#!/bin/sh
# Run a command inside WSL Ubuntu from Git Bash, in the WSL path of the current folder.
# Usage: tools/wsl.sh <command...>     e.g. tools/wsl.sh tools/check.sh dev -- ./build/pipeline
here=$(pwd -W 2>/dev/null || pwd)
tools=$(cd "$(dirname "$0")" && pwd -W 2>/dev/null || pwd)
lin=$(MSYS_NO_PATHCONV=1 wsl -d Ubuntu -- wslpath -a "$tools/_inwsl.sh")
exec env MSYS_NO_PATHCONV=1 wsl -d Ubuntu --cd "$here" -- bash "$lin" "$@"
