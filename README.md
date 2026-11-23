# Shopping Sessions

**An entry for the Databricks "Shopping Sessions" track of the [Builder's Buildathon](https://www.joinbuilders.org/buildathon/tracks/shopping-sessions).**

The task is to turn a large, messy log of website activity into two clean daily reports, as fast as possible. Our solution is one C++ program that does the whole job in about 1.27 seconds on 20 million events. The organizers' Python starter took about 173 seconds on the same data.

- [The problem](#the-problem)
- [Our solution](#our-solution)
- [Tech stack](#tech-stack)
- [Results](#results)
- [How to run it](#how-to-run-it)
- [What's in this repository](#whats-in-this-repository)

---

## The problem

### The story

An online shop logs everything its visitors do: viewing a product, adding it to the cart, buying it. It also logs every time someone clicks an ad from a marketing campaign (an email, a search ad, a social post, and so on). The shop wants two questions answered, day by day:

1. **For each customer:** how many separate visits ("sessions") did they make, how much did they do, how much did they spend, and which product category did they spend the most on?
2. **For each marketing campaign:** how many visits did it bring in, how many of those ended in a purchase, how much revenue did that produce, and how many other visits did it help along?

The data to answer these is deliberately messy, the way real logs are.

### The input

The program reads a folder of CSV files (comma-separated text):

| File(s) | What each line is |
| --- | --- |
| `events/*.csv` | One action on the site: who did it, when, what kind (`view`, `cart`, `purchase`), which product, and the price for purchases |
| `clicks/*.csv` | One click on an ad: who clicked, which campaign, and when |
| `links/*.csv` | Two user IDs that belong to the same person (for example, the same shopper on a phone and a laptop) |
| `products.csv` | Each product's category |
| `campaigns.csv` | Each campaign's channel (email, search, ...) |
| `manifest.json` | The 14-day time window the reports cover |

### What makes it hard

The full rules are in [`rules.md`](rules.md), and they are strict: the output must match the answer key exactly, row for row. In plain terms:

- **One person, many IDs.** A shopper can appear under several user IDs (cleared cookies, a second device). The `links` files say which IDs belong together, and the chains can run hundreds of links long. Every count is per *person*, not per ID.
- **Duplicates.** Many events were recorded more than once. Only the earliest-recorded copy counts, even if it turns out to be invalid and a later copy looks fine.
- **Dirty data.** Lines can be broken, fields can be quoted or empty, and IDs differ by upper or lower case. Timestamps come in three formats, some of them invalid (February 30, hour 24, ...). Prices and product types can be junk too. Each kind of bad data has a precise rule for what to drop.
- **Sessions.** A person's events are sorted by time, and a gap of more than 30 minutes starts a new session. Each session counts on the calendar day (UTC) on which it started.
- **Credit and assists.** Each session is credited to the person's most recent ad click in the 7 days before it started, or to `organic` if there was none. Every campaign the person clicked in that 7-day window counts as having *assisted* the session.

### The output

Two tables, written as [Parquet](https://parquet.apache.org/) files (a standard compact, column-based data format):

- **`user_daily`**: one row per person per day, with `user_id`, `day`, `sessions`, `events`, `revenue_cents` and `top_category`.
- **`campaign_daily`**: one row per campaign per day, with `campaign_id`, `channel`, `day`, `sessions`, `purchasing_sessions`, `revenue_cents` and `assisted_sessions`.

### How it is scored

Each submission is a Docker container. The scorer runs it on a machine with 8 virtual CPUs and 24 GB of memory and times how long the whole run takes. The output is first checked against a public 1-million-event test set, and any wrong row disqualifies the submission. The practice leaderboard used about 20 million events. The final round uses about 60 million events (several gigabytes of CSV) and re-times the top entries on fresh datasets.

**The goal is to produce exactly the right answer in the shortest possible time.**

---

## Our solution

The starter kits use general-purpose data tools (PySpark, DuckDB, Polars). We wrote everything from scratch in one C++ program, [`src/main.cpp`](src/main.cpp), with no third-party libraries. That let us hand-tune every step for this exact data and these exact rules.

The program runs in phases. All 8 CPUs share the work within each phase:

1. **Read the lookup tables.** Products and campaigns go into fast in-memory lookup tables.
2. **Connect linked IDs.** One thread reads the `links` files and merges IDs into people with a [union-find](https://en.wikipedia.org/wiki/Disjoint-set_data_structure) structure. Each person is named by their alphabetically smallest ID, as the rules require. Meanwhile, the other threads start on step 3.
3. **Parse events and clicks.** The files are read in 256 KB pieces, small enough to stay in the CPU's fast cache. A custom CSV scanner uses AVX2 vector instructions to check 32 bytes at a time, and a custom timestamp parser checks all three formats with full calendar validation. Each event is reduced to a small fixed-size record.
4. **Remove duplicate events.** Records are split into 1,024 groups ("shards") by event ID. Each shard is deduplicated separately in a hash table that keeps the earliest copy, and only then is that copy validated.
5. **Group by person.** The surviving events and clicks are looked up in the linked-ID table and regrouped into shards by person.
6. **Build sessions.** Within each shard, each person's events are sorted by time and split into sessions. Each session is credited to the right ad click, and a sliding 7-day window tracks which campaigns assisted it. The results are summed per day.
7. **Write the output.** We wrote our own minimal Parquet writer, which streams each table straight to disk.

Some lower-level choices that made a measurable difference:

- **Memory layout.** Records live in 16 KB blocks carved from one large memory region backed by 2 MB "huge pages". Blocks are reused from one phase to the next, so later phases never have to ask the operating system for fresh memory.
- **Hashing instead of string comparison.** IDs are compared by 64-bit hashes plus their length, so each event doesn't have to look up its original text. The chance of a hash collision changing the result is about 1 in 10 million at 60 million events.
- **Profile-guided optimization (PGO).** While building the Docker image, the compiler first builds a test version of the program and runs it on a generated 1-million-event dataset. It then recompiles using that run's profile, so the code is laid out for the paths that actually run most often.
- **A minimal container.** The final image is built `FROM scratch` and contains only the single statically linked program, with no operating system, so there's nothing extra to start up.

[`SOLUTION.md`](SOLUTION.md) explains each step in more depth, including how we validated our trade-offs.

---

## Tech stack

| Part | Tool |
| --- | --- |
| Pipeline | C++20, one file, no external libraries; compiled with GCC 13 (`-O3`, AVX2, tuned for Intel Cascade Lake) |
| Speed-ups | AVX2 vector instructions, profile-guided optimization, huge pages, `pread` input with kernel read-ahead |
| Output format | Parquet, written by our own minimal writer |
| Packaging | Docker, multi-stage build: Rust 1.85 builds the data generator for PGO training, `gcc:13` compiles, and the final image is `FROM scratch` |
| Test data | The organizers' data generator, written in Rust (in [`generator/`](generator/)) |
| Testing | Python 3 with `polars` (fixture runner and output comparison scripts) |

---

## Results

### Correctness

| Check | Result |
| --- | --- |
| 36 hand-written edge-case tests ([`fixtures/`](fixtures/)) | 36 / 36 pass |
| Public 1M-event test set vs. the official answer ([`dev-answer/`](dev-answer/)) | exact match |
| Same output whether run on 1 thread or many | identical |
| The rules' built-in consistency checks on freshly generated datasets | pass |

### Speed (official scorer, practice data of about 20 million events, 8 vCPUs)

| Pipeline | Time |
| --- | --- |
| Baseline (organizers' PySpark starter) | 172.90 s |
| **Ours** | **1.2730 s** |

That is about **136× faster** than the baseline.

Note: the program also writes a second copy of each `user_daily` file with a `_dup-` prefix (left over from an experiment measuring output-writing cost). Files starting with `_` are ignored by the scorer, so these copies are harmless.

---

## How to run it

### Option A: with Docker (exactly as the scorer runs it)

You need Docker with `buildx`, plus Python 3.10+ with `polars` (`pip install polars`) for the test scripts.

```sh
make build        # build the image (compiles with profile-guided optimization)
make fixtures     # run all 36 edge-case tests inside the container with the scorer's limits
```

To run on the 1-million-event test set, first build the data generator (needs Rust 1.85+), then generate the data, run, and check:

```sh
(cd generator && cargo build --release)
export PATH="$PWD/generator/target/release:$PATH"

make dev          # generate the dev data into dev/ (seed 1, 1M events)
make run-dev      # run the container on dev/ with the scorer's flags and time it; output goes to out/
make check-dev    # compare out/ with the official answer in dev-answer/
```

If your machine can't spare 8 CPUs and 24 GB, lower the limits: `make run-dev CPUS=4 MEM=8g`.

### Option B: without Docker (Linux or WSL)

You need GCC 13+ (C++20) on an x86-64 CPU with AVX2 (most Intel and AMD CPUs from 2015 on).

```sh
./build.sh                                   # compiles src/main.cpp to build/pipeline
./build/pipeline path/to/data path/to/output # or set DATA_DIR and OUTPUT_DIR instead of passing arguments
```

Testing without Docker:

```sh
python3 scripts/run_fixtures.py fixtures -- ./build/pipeline   # all 36 edge-case tests
python3 scripts/compare.py path/to/output dev-answer            # compare a dev-set run with the official answer
```

Useful settings:

| Environment variable | Effect |
| --- | --- |
| `NCPU=4` | Number of worker threads (default: all CPUs) |
| `SS_PROF=1` | Print a timing for each phase to stderr |

---

## What's in this repository

| Path | Contents |
| --- | --- |
| [`src/main.cpp`](src/main.cpp) | The entire pipeline |
| [`src/pgo_gen.cpp`](src/pgo_gen.cpp) | A small helper linked only into the profiling build (for profile-guided optimization) |
| [`Dockerfile`](Dockerfile) | The 3-stage build: generator → profile-guided compile → minimal final image |
| [`SOLUTION.md`](SOLUTION.md) | Detailed design notes, trade-offs, and scorer history |
| [`rules.md`](rules.md) | The official problem rules (the source of truth) |
| [`fixtures/`](fixtures/) | 36 edge-case tests, each an `input/` folder and its `expected/` output |
| [`dev-answer/`](dev-answer/) | The official answer for the 1M-event dev set |
| [`generator/`](generator/) | The organizers' data generator (Rust) |
| [`scripts/`](scripts/) | Run the container like the scorer, run the fixtures, compare outputs |
| [`tools/`](tools/) | Helpers we used for development on Windows via WSL, plus benchmarking and test-data tools |
| [`starters/`](starters/) | The organizers' original starter kits (DuckDB, Polars, PySpark), kept for reference |
| [`Makefile`](Makefile), [`build.sh`](build.sh) | Build and test shortcuts |
| [`TEAM.md`](TEAM.md) | Team information |
