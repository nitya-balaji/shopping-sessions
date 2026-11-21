# Shopping Sessions: strategies and shared workflow

Score = container wall time (start to exit) on ~60M events, 8 vCPU / 24 GB. Correctness is a gate:
any wrong row on the 1M dev set rejects the submission. Board best on practice data: 0.30 s. Baseline: 172 s.

## Branches (one parallel session each, in its own git worktree)

| Branch | Approach | Role |
| --- | --- | --- |
| `polars-safe` | Python + Polars/DuckDB, custom line parsing to follow `rules.md` exactly | Safe first submission and the correctness oracle for the other branches |
| `cpp-mt` | C++20, mmap + parallel chunked hand parser, ID interning, union-find, partition-by-person, hand-written Parquet writer, static binary | Main fast contender |
| `cpp-extreme` | C++20, low-level: SIMD (AVX2, AVX-512 where available) field scanning, hash-keyed IDs, radix sorts, zero-copy, per-thread arenas, minimal image (`FROM scratch`) | Goes after the 0.30 s board top |

The best correct, fastest branch is merged into `main`.

## Local environment (this machine)

- Docker does not work here. Everything is built and run **natively in WSL Ubuntu** (6 cores, ~3 GB RAM). The scorer has 8 vCPUs, 24 GB.
- WSL has: gcc/g++, make, Rust (only for the generator; solutions must not use Rust), Python venv `~/ssenv` with polars, duckdb 1.5.6, pyarrow.
- No sudo in WSL: you cannot `apt install`. Header-only / vendored C++ deps only (or write it yourself).
- Datasets live in WSL, not in git: `~/ssdata/dev` (seed 1, 1M events, matches `dev-answer/`), `~/ssdata/b5m` (seed 7, 5M events). More: `tools/gen.sh SEED EVENTS`.

## Commands (from Git Bash, at the repo root)

```sh
tools/wsl.sh <cmd...>                                  # run anything in WSL at this folder
tools/wsl.sh tools/check.sh fixtures -- <CMD...>       # 36 edge-case fixtures
tools/wsl.sh tools/check.sh dev -- <CMD...>            # 1M dev set: exact diff vs dev-answer/ + wall time
tools/wsl.sh tools/check.sh bench -- <CMD...>          # 5M timed run; machine-wide lock so runs don't overlap
tools/wsl.sh tools/check.sh all -- <CMD...>
tools/wsl.sh python scripts/compare.py OUT_A OUT_B     # exact diff of two output folders
```

`<CMD>` gets `DATA_DIR`, `OUTPUT_DIR`, `SCRATCH_DIR`, `NCPU`, `MEM_GB` as env vars and data/out as its last two args.
Outputs land in `~/ssout/`.

## Rules for every branch

1. `rules.md` is the spec. Read all of it and `fixtures/README.md` before coding.
2. The submission is the repo root: a `Dockerfile` (linux/amd64, runs as uid 1000, read-only root, no network at run time,
   CMD with no args reads `$DATA_DIR`, writes `$OUTPUT_DIR/user_daily/*.parquet` and `$OUTPUT_DIR/campaign_daily/*.parquet`).
   It can't be tested locally (no Docker), so keep it simple and mirror the starter's conventions.
3. The scorer's CPU is Intel Xeon Cascade Lake or newer (AVX-512). Local CPU may differ: check `/proc/cpuinfo`; any
   AVX-512 path needs runtime dispatch or a fallback that is tested locally.
4. Never check data into git (repo limit 50 MB). Commit on your own branch only.
5. Done means: 36/36 fixtures, dev diff `MATCH`, a bench time, and a `RESULTS.md` at the root with those numbers.
