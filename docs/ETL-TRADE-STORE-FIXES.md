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

Planned 2026-11-22 in two plans, executed in order:

1. `docs/plans/2026-11-22-P1-arrow-fork-pyarrow.md`: the fork built with the components pyarrow
   needs (jemalloc, as stock pyarrow), pyarrow's Cython taught PCODEC (part of `FORK.patch`),
   a pyarrow 26.0.1+tparquet.1 wheel for CPython 3.14 built on rey (`make -C cpp pyarrow
   PYTHON=...`), and the C++ query tool `bench/trade-store/cpp/tpq_query` (a thin CLI over
   `arrow::dataset`).
2. `docs/plans/2026-11-22-P2-trade-store-gate.md`: the ETL harness moved into
   `bench/trade-store/`, gate files regenerated from the Massive CSVs through a snapshot of
   ETL's transform, writer parity against the tparquet Rust writer, a three-way correctness
   check (Massive → transform = stock pyarrow on Z256k = forked pyarrow = C++), the cold HDD
   run at 4M with a diagnosis of every reader, and a PASS/FAIL report. It stops there.

Settled with the user on 2026-11-22:

- The benchmarked branches are already on `main` (`dd3e2d3`, `b1367d1`).
- **One engine, Arrow C++, for reading and writing**, from C++ and Python; pyarrow is the thin
  Python wrapper. Rust remains only inside the codec. The tparquet Python reader and writer are
  not ETL dependencies; the reader is dropped if the gate passes. Codecs are a fixed per-column
  map (from the Rust writer's auto choice on the same data); a per-column codec benchmark comes
  later.
- **No portable wheel.** Build on rey for the newest CPython 3.14 and rebuild for new releases;
  obiwan and yoda (Ubuntu 24.04, glibc 2.39) get it after their move to 26.04. How ETL pins it
  is ETL's decision when it adopts the wheel (per-host builds and hash pins conflict).
- **Spinpool only, recordsize 4M**; NVMe is not measured. A recordsize sweep comes later.
- **Targets are relative to the same run** (stock pyarrow on Z256k, interleaved), not the
  absolute numbers below.
- The coalesced-reads guard, the polars filter-spin fix and the capped `make python-mutants`
  are **deferred**: they matter only if the gate fails and the Rust reader comes back. If
  that happens, the `take_pending` fix restructures the split (`needed.min(front.num_rows())`,
  push back only a non-empty remainder) so no equivalent mutant survives, asserts the
  `pending_rows` invariant, and tests it deterministically with a small RSS backstop; mutants
  run under `membound` (12G per mutant, 48G per run, `--timeout 400`, `-j 2`).
- Stock pyarrow **aborts the process** on a PCODEC file (`Unsupported compression codec value:
  20547`); ETL checks `pa.Codec.is_available("pcodec")` at startup.

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

## Deferred bug: unbounded memory growth in coalesced reads (and an uncapped mutants run)

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

The gate of `docs/plans/2026-11-22-P2-trade-store-gate.md` (D8) passes on 2026-10-26, HDD cold
at 4M, medians of 5 rounds, every ratio against stock pyarrow on Z256k in the same run:

- values identical along Massive CSV → ETL transform → stock pyarrow (Z256k) → forked pyarrow
  and C++ (T256k, TS256k), every query;
- the forked pyarrow writer's T256k within 1% of the tparquet Rust writer's size;
- forked pyarrow on T256k: full ≤ 1.1×, cols3 ≤ 1.1×, tickers10 ≤ 1.0× Z256k;
- TS256k full ≤ 1.1× T256k;
- pyarrow within 10% of the C++ tool on the same queries (Python overhead);
- the wheel rebuilds on rey from `make -C cpp pyarrow PYTHON=...`.

Then rerun the ETL bench (Tasks 6c–9) with the forked pyarrow as the tparquet reader.
