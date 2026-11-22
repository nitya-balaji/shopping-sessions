# Shopping Sessions: native C++ solution (team UHH)

One static C++20 binary (`src/main.cpp`, no dependencies) that implements `rules.md` exactly and writes
both Parquet tables with its own minimal Parquet writer. The image is built with profile-guided
optimization (see `Dockerfile`) and runs `FROM scratch` with just the binary.

## Scorer history (practice data, ~20M events, 8 vCPUs)

| Commit | Change | Time |
| --- | --- | --- |
| d08143e | first submitted version | 1.61 s |
| 7b13de0 | faster writer, timestamps, parser | 1.50 s |
| 16e478f | transparent huge pages for record memory | 1.33 s |
| 2dcc3ff | input chunks pre-mapped (MADV_POPULATE_READ) | 1.30 s |
| 9b29c2d | background readahead + profile-guided build | 1.28 s |
| 65656f4 | compiled for Cascade Lake | 1.27 s |

Baseline (PySpark starter): 172.90 s.

## Correctness checks (run after every change)

| Check | Result |
| --- | --- |
| 36 edge-case fixtures | 36/36 |
| 1M dev set vs `dev-answer/` | exact MATCH |
| Byte-identical output vs the previous version on seeds 11, 29, 2028 (1M) and seed 7 (5M) | MATCH |
| Same output with 1, 2 and 3 threads | MATCH |
| rules.md self-checks (session/revenue sums, assists) | OK |

## How it works

1. **Small tables.** `products.csv` and `campaigns.csv` go into a `KeyIndex`: IDs of the form
   `<prefix><number>` (`p123`, `c_45`) map through a flat array (digits validated and converted 8 at a
   time); any other ID uses an exact hash map. Category indices follow byte order, so the `top_category`
   tie-break is "smallest index".
2. **Input.** Event and click files are read with `pread` in 256 KB chunks into a reused per-thread
   buffer, so each chunk is still in L2 while it is parsed and no file pages are mapped. A line belongs to
   the chunk holding its first byte; each file's header is skipped. The few bytes that must outlive the
   buffer (user ids of valid events, click ids) are copied into a per-thread string area. A background
   thread asks the kernel to read all inputs ahead (`posix_fadvise(WILLNEED)`) in case they are not cached.
3. **CSV dialect.** `scan_lines<K>` loads each line's first 128 bytes with AVX2, takes the comma, quote and
   newline bitmasks, and accepts a line iff it has exactly K-1 commas and every quote is the first or last
   byte of a field wrapped in a pair of quotes (`""` = null, `\r` is data, empty line = malformed).
   Longer lines use the exact scalar parser.
4. **Timestamps** F1/F2/F3 are parsed by hand with full calendar checks (calendar tables for 1900-2199,
   SWAR digit validation, 8-digit SWAR conversion for epoch milliseconds, ASCII digits only).
5. **Links** are parsed and joined by union-find on one thread while the other threads parse events and
   clicks. The root of each set is its byte-wise smallest ID, so it names the person.
6. **Event dedup.** Each parsed copy (valid or not) goes to a shard by `event_id` hash; per shard a hash
   table keeps the copy with the smallest `ingest_ts`, and only then is validity checked (dedup before
   validation). Click duplicates are identical, so they cannot change any output.
7. **Persons.** Surviving events and valid clicks look up their person (prefetched, 32-byte table slots)
   and are re-sharded by person. Shards: 512, or 1024 above ~10M events so each shard stays cache-sized.
8. **Sessions.** Per shard, a small hash table groups records by person (counting sort), each person's
   events are sorted by ts (insertion sort up to 32), and one pass builds sessions (gap > 30 min, equal
   timestamps share), credits the latest click within 7 days (tie: smallest click_id) and keeps a sliding
   7-day window of distinct campaigns for assists.
9. **Memory.** All shuffled records live in append-only 16 KB blocks from one arena on transparent huge
   pages, written with streaming stores; blocks are recycled between phases (raw copies -> person events
   -> output rows), so later phases touch no fresh memory.
10. **Output.** Per-thread `user_daily` Parquet files and one `campaign_daily` file (PLAIN, uncompressed,
    `day` as `DATE`), streamed column by column.

## Known trade-offs (hash identities)

To avoid touching a random input line per event, three identities use hashes instead of byte comparison:

- **Persons inside a shard:** 64-bit hash of the name + its length.
- **Linked-ID lookup:** 64-bit hash of the user ID + its length.
- **Event dedup:** 64-bit hash of `event_id` + an independent 16-bit check value.

A wrong result needs a full collision of one of these: on the order of 1e-7 at 60M events. Record
timestamps are u32 ms offsets from `window_start - 7 days`; windows longer than 42 days stop the run with
a message (the generator always uses 14 days).

## Commands

```sh
./build.sh                                              # in WSL/Linux: build/pipeline
tools/wsl.sh sh build.sh                                # same, from Git Bash on Windows
tools/wsl.sh tools/check.sh all -- ./build/pipeline     # fixtures + dev diff + 5M benchmark
SS_PROF=1 ./build/pipeline DATA OUT                     # per-phase timings on stderr
```
