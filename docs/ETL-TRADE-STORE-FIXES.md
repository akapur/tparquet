# tparquet fixes needed for the ETL trade store

Written 2026-11-22 from the ETL trade-store format bench (`~/Development/ETL`, plan
`docs/plans/2026-11-16-trade-store-format-tuning-bench-plan.md` in the
`.claude/worktrees/trade-store-format-bench` worktree). The ETL side is paused: its decision
rule (D11) and final report (Task 10) wait until this work lands and the bench is rerun.

## Goal

Read tparquet files from **C++ and Python** as fast and as safely as pyarrow reads the
equivalent zstd Parquet file (`Z256k`), with none of the bugs below.

## Why tparquet at all

The only reason is PCODEC. On a trade day it makes files 14–22% smaller than the best
standard Parquet we could tune (2026-10-26: 1142 MB vs 1328 MB; 2018-06-13: 281 MB vs 361 MB).
About 95% of the gap is pco's entropy coding of `sip_timestamp`, the participant/trf timestamp
offsets, `price_e6` and `sequence_number`; no Parquet encoding + zstd gets within 2% of it.
A DIY pco container was considered and rejected: it has the same lock-in and more code to own.

## Decision: one reader for both languages, the Arrow C++ fork

tparquet has three read paths, and all the bugs found are in one of them:

| path | built on | status |
|---|---|---|
| Python (`tparquet`, `tparquet.polars`, `polars_tparquet`) | tparquet's Rust reader (`python/rust/file.rs`) + `polars.py` | slow ticker and full reads, the bugs below |
| C++ (`tparquet::ReadTable`, every `parquet::arrow` API) | Apache Arrow C++ 26.0.1 + `cpp/arrow/FORK.patch` (codec only) | Arrow's own reader; never measured on our files |
| C (`libtparquet_read`, `rust/cread`) | plain arrow-rs `ParquetRecordBatchReader` | not used by ETL |

Chosen 2026-11-22: **both C++ and Python read through the Arrow C++ fork.** Python gets it by
building **pyarrow 26.0.1 against the forked libarrow** (same Arrow version as the fork). Arrow
C++ already does column-parallel decode, read coalescing, I/O separate from decode threads and
row-group statistics pruning, so C++ and Python share one hardened reader and cannot diverge.
Polars consumes pyarrow tables zero-copy (`pl.from_arrow`).

The tparquet Rust reader's page-index and column-parallel work (deferred list below) is done
only if the forked pyarrow falls short of the targets.

## Work, in order

1. **Land the benchmarked branches on the fork's `main`.** The bench wheel was built from the
   local, unpushed `bench-integration-2026-11-16` (HEAD `50be452a`), merging
   `python-parallel-decode`, `python-coalesced-reads`, `python-string-stats` and
   `python-writer-options`. The writer and the `tparquet` Python package still come from here.
2. **Build the Arrow C++ fork on rey** (`cpp/arrow/prepare.sh`, `deps.sh`, `build.sh release`,
   see `cpp/arrow/FORK.md`). The build needs `libtparquet_capi.a` from `rust/capi`.
3. **Build pyarrow 26.0.1 against it.** From `target/cpp/arrow-src/python` with
   `ARROW_HOME`/`CMAKE_PREFIX_PATH` pointing at the fork's install, `PYARROW_WITH_PARQUET=1`
   (and `PYARROW_WITH_DATASET=1`; the fork build must then include Arrow's dataset/acero
   components, so extend `build.sh` if they are off). Build it as a wheel that bundles the
   fork's `libarrow`/`libparquet` (`PYARROW_BUNDLE_ARROW_CPP=1`) so it installs without
   `LD_LIBRARY_PATH`. Build for **CPython 3.14** (ETL requires `>=3.14.3`); the bench venv is
   3.13, so build both or move the bench to 3.14. Add a make target and a script that
   reproduces the wheel, and a smoke test that `pyarrow.parquet.read_table` on a PCODEC file
   equals `tparquet.read_table` (canonical JSON).
4. **Decide distribution.** The wheel must install on rey and the other hosts (obiwan, yoda):
   one manylinux-style build hosted somewhere uv can reach (a local index or a `find-links`
   directory), pinned by hash in ETL. Write the rebuild recipe for an Arrow version bump.
5. **Bench C++ and the forked pyarrow** on T256k and TS256k with the ETL harness (see
   *Benchmark data*): the `pa`, `pa_nopb`, `pa_coal` and `pa_mem` readers become valid for
   the tparquet formats once pyarrow reads PCODEC (`TPQ_FMTS` and `readers_for` in
   `readbench.py`). Add a small C++ reader (`tparquet::ReadTable` and
   `parquet::arrow::FileReader` with a row-group filter for tickers) with the same queries and
   timings, to confirm C++ matches pyarrow.
6. **Python-side fixes kept regardless** (cheap, and in code that ships):
   - **Polars per-batch filter spin (~20 lines).** `tparquet/polars.py` `_source` (~L282-288)
     runs `pl.DataFrame(batch).filter(predicate)` per 64K-row batch; Polars spreads each tiny
     filter over 64 threads and the workers spin: 68 batches cost 1.79 s CPU at 64 threads,
     0.012 s at 1. Fix: mask on the predicate columns only, skip batches with no match, filter
     the rest once. Readers must filter (`polars/io/plugins.py:92`), so don't yield unfiltered.
     Prototype: CPU 2.27 s → 0.74–0.89 s.
   - **The memory bug below.**

## Bug: unbounded memory growth in coalesced reads (and an uncapped mutants run)

`Decoded::take_pending` (`python/rust/file.rs:276`, branch `python-coalesced-reads`). Under
`cargo mutants`, the `front.num_rows() > needed` → `>=` mutant grows memory at ~7 GB/min in
`test_conformance`; cargo-mutants' auto timeout (~745 s) lets it reach tens of GB. Three
uncapped `-j 8` mutants runs are the likely cause of an OOM crash on rey (2026-11-16).

Mechanism: with `>=`, an exactly-consumed batch is pushed back as a zero-row slice. Each slice
holds the parent batch's buffers alive, `pending` keeps accumulating them, and nothing bounds
`pending` or checks `pending_rows` against its contents. The shipped comparison does not leak
as far as we know, but the code's memory safety rests on one comparison with no guard.

Fix:
- Never push empty slices back; drop zero-row batches on entry to `pending`.
- Assert `pending_rows == sum(pending[i].num_rows())` (debug) and bound `pending` (never more
  than about one decoder batch beyond `batch_size`).
- Add a test that streams a large file and checks peak RSS stays bounded.
- Cap each mutant in `make python-mutants`: run the Python suite through a memory-capped
  wrapper (pattern: `~/tmp/tparq-wheels/py12g.sh`, `membound 12G`), `--timeout 400`, `-j 2`,
  and wrap the whole run in `membound 48G`. Raise the missing cap upstream.

## Deferred: tparquet Rust reader fixes (only if the forked pyarrow misses the targets)

Line references are on `bench-integration-2026-11-16`.

1. **Page index and page skipping (~120 lines Rust + ~60 Python).** Files already carry a page
   index; the reader never loads it (`ArrowReaderOptions::new()`, `python/rust/file.rs:218`).
   Enable `with_page_index`, expose per-page bounds to Python, build `RowSelection`s from
   `_test` (`polars.py:168`) and pass them to the push decoder (`push_decoder/mod.rs:303-351`).
   pco has no random access inside a page; the unit is a page (~20K rows).
2. **Column-parallel decode within a row group (~200 lines Rust).** Today one thread per row
   group (`file.rs:354-405`), columns serial. One push decoder per leaf on its own thread with a
   precomputed `RowSelection`, then zip batches.
3. **Read concurrency tied to decode threads.** Full reads: T256k 2.15 s at 64 threads vs 1.72 s
   at 16, and the default is one per CPU. Probable cause (unmeasured): one read in flight per
   row group being decoded, so the HDD seeks. Cap in-flight I/O separately.
4. **In-memory sources slower than disk.** `read_table(WholeFile(bytes))` 2.73 s vs 1.72 s from a
   file (TS256k 5.32 s vs 4.04 s).
5. **Coalesced reads over-read.** cols3 fetches 1522 MB for 551 MB needed (2.8×; pyarrow on
   Z256k 2.2×). Make the gap thresholds tunable.

Also open, whichever reader: **TS256k full reads are 2.4× slower than T256k** on the Rust
reader (4.04 s vs 1.72 s) with the same CPU, bytes and row-group count. Hypothesis: uneven
ticker-snapped row groups serialise a row-group-parallel reader. Arrow C++ decodes columns in
parallel, so check whether the forked pyarrow still shows it.

## Where the Rust reader loses today (HDD cold, recordsize 4M, 2026-10-26, medians)

| query | T256k (tparquet Rust) | Z256k (pyarrow) |
|---|---|---|
| full | 1.72 s (t16), 2.15 s (t64) | 1.37 s |
| cols3 | 1.56 s, 2.8× read amplification | 1.44 s, 2.2× |
| ticker1 | 0.14 s | 0.09 s |
| tickers10 | 0.86 s | 0.62 s |
| TS256k full | 4.04 s | — |

Warm, 10 tickers: T256k 0.46 s wall / 2.2 s CPU; polars-native on Z256k 0.065 s / 0.22 s.

## Benchmark data

- **Massive flat-file cache (raw source).** `~/Development/ETL/tmp/massive/trades_v1/`
  (gitignored, ~7.5 GB, **never delete it**): full-day `*.csv.gz` for 2003-09-10, 2005-06-15,
  2006-06-14, 2007-03-14, 2010-06-16, 2016-06-15, 2018-06-13, 2026-10-22 and 2026-10-26.
  Use these instead of downloading again; add days to the same cache if needed.
- **Converted sources.** `~/tmp/tparq-bench/{2010-06-16,2018-06-13,2026-10-26}.arrow` (Arrow
  IPC; memory-map it; `with_offsets` in `~/Development/ETL/tmp/fmtbench/common.py` turns the
  participant/trf timestamps into offsets as the store does).
- **Bench files.** `~/tmp/tparq-bench/readset/<day>.<fmt>.parquet` for fmt in base, Z, Z256k,
  S256k, PT, T256k, TS256k (2018-06-13 and 2026-10-26). T256k/TS256k are the tparquet files;
  Z256k is the zstd reference.
- **Harness.** `~/Development/ETL/tmp/fmtbench/` (`readbench.py`, `run_bench.sh`,
  `summarize.py`, `summary.md`, ticker lists `tickers_<day>.json`). Python with
  tparquet/polars/pyarrow: `~/tmp/fmtbench-venv/bin/python` (3.13). Cold HDD/NVMe runs use
  the `/bench/hdd` and `/bench/nvme` ZFS datasets (primarycache=metadata).
- **Warm profiling scripts** from this investigation (may be cleaned up with the job):
  `~/.claude/jobs/458af624/tmp/` (`warm_tickers.py`, `breakdown.py`, `conv.py`, `agentA/`).
- `perf` is unavailable on rey (`perf_event_paranoid=4`).

Run anything large under `membound` (e.g. `membound 24G python ...`); rey's user sessions share
a 200 GB cap. Arrow and pyarrow builds are memory-hungry at high `-j`: cap them too.

## Done when

On the 2026-10-26 files, HDD cold at 4M:

- forked pyarrow and C++ read PCODEC files with values identical to `tparquet.read_table`;
- forked pyarrow, T256k: full ≤ 1.1 × Z256k's 1.37 s; tickers10 ≤ Z256k's 0.62 s;
  cols3 ≤ 1.1 × Z256k's 1.44 s;
- C++ within 10% of the forked pyarrow on the same queries;
- TS256k full within 10% of T256k;
- the forked pyarrow wheel installs on rey, obiwan and yoda from a reproducible build;
- the polars filter-spin fix, the coalesced-reads guard and its test are in, and
  `make python-mutants` runs with a per-mutant cap.

Then rerun the ETL bench (Tasks 6c–9) with the forked pyarrow as the tparquet reader.
