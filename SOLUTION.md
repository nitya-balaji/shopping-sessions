# Shopping Sessions: native C++ solution

One static C++20 binary (`src/main.cpp`, no dependencies) that implements `rules.md` exactly and writes
both Parquet tables with its own minimal Parquet writer. The run image is `FROM scratch` + the binary.

## Results (this laptop: WSL2, Ryzen 5 4500U, 6 threads, ~3 GB RAM)

| Check | Result |
| --- | --- |
| 36 edge-case fixtures | 36/36 |
| 1M dev set vs `dev-answer/` | exact MATCH, ~0.6-0.9 s |
| Same output with 1 thread and 6 threads | MATCH |
| Seeds 11, 29, 2028 (1M each): rules.md self-checks | all OK |
| 5M events (seed 7) | ~1.0-1.3 s in-process (1.3-2.0 s wall incl. shell), noisy laptop |

Rough projection for the 60M final on 8 Xeon vCPUs: ~10-15 s (well under the 3-min cap).
The Dockerfile could not be built here (Docker is not working on this machine): build it once with
`make build` and run `make fixtures` / `make check-dev` before submitting, or rely on the scorer's dev gate.

## How it works

1. **Small tables.** `products.csv` and `campaigns.csv` go into a `KeyIndex`: IDs of the form
   `<prefix><number>` (`p123`, `c_45`) map through a flat array; any other ID uses an exact hash map.
   Category indices follow byte order, so the `top_category` tie-break is "smallest index".
2. **Parallel parse.** Every input file is `mmap`ed and cut into 4 MB chunks; a line belongs to the chunk
   holding its first byte, and each file's first line (header) is skipped. `scan_lines<K>` finds `,` `"`
   `\n` 64 bytes at a time with AVX2 and runs the rules' CSV state machine (quotes only around a whole
   field, `""` = null, `\r` is data, wrong field count / empty line = malformed).
3. **Timestamps** F1/F2/F3 are parsed by hand with full calendar checks (SWAR digit validation, ASCII
   digits only, F3 overflow = outside the window).
4. **Links** are parsed and joined by union-find on one thread *while* the other threads parse events
   and clicks. The root of each set is its byte-wise smallest ID, so it names the person.
5. **Event dedup.** Each parsed copy (valid or not) is sent to a shard by `event_id` hash; per shard a
   hash table keeps the copy with the smallest `ingest_ts`, and only then is validity checked
   (dedup before validation). Click duplicates are identical, so they cannot change any output.
6. **Persons.** (records live in append-only 16 KB blocks from per-phase arenas: no vector growth or copies; the raw-copy arena is freed right after dedup)
    Surviving events and valid clicks resolve their user ID to the person name (prefetched
   hash lookups) and are re-sharded by person.
7. **Sessions.** Per shard, events sort by (person, ts) and clicks by (person, ts, click_id). One pass per
   person builds sessions (gap > 30 min, equal timestamps share), credits the latest click within 7 days
   (tie: smallest click_id), and keeps a sliding 7-day window of distinct campaigns for assists.
8. **Output.** Per-thread `user_daily` Parquet files and one `campaign_daily` file (PLAIN, uncompressed,
   `day` as `DATE`).

## Known trade-off

Inside a person shard, persons are told apart by a 64-bit hash of their name plus its length, not by
byte comparison (comparing bytes would touch a random input line per event). A wrong merge needs two
different equal-length names with the same 64-bit hash in the same shard: probability about 1e-9 at 60M events.

## Commands

```sh
./build.sh                                              # in WSL/Linux: build/pipeline
tools/wsl.sh sh build.sh                                # same, from Git Bash on Windows
tools/wsl.sh tools/check.sh all -- ./build/pipeline     # fixtures + dev diff + 5M benchmark
SS_PROF=1 ./build/pipeline DATA OUT                     # per-phase timings on stderr
```
