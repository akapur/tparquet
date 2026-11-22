# P2 — Trade-store gate: correctness from the Massive files, and the cold HDD benchmark

## Goal and scope

Decide by measurement whether forked pyarrow and the C++ library (Arrow C++ 26.0.1 + PCODEC,
built by `docs/plans/2026-11-22-P1-arrow-fork-pyarrow.md`) read PCODEC trade files correctly and
as fast as stock pyarrow reads the equivalent zstd file. Correctness is anchored to the Massive
flat files. The plan ends with a report and **stops**: it never starts a fix.

**In scope:** the ETL bench harness copied into `bench/trade-store/harness/`; a snapshot of the
ETL transform in `bench/trade-store/etl_snapshot/`; regenerating the gate files from the Massive
CSVs; writer parity against the tparquet Rust writer; a three-way correctness check; the cold
HDD read benchmark at recordsize 4M with a diagnosis of every (format, query, reader); the gate
report.

**Out of scope:** NVMe (the store will not live on NVMe); other recordsizes and a per-column
codec benchmark (later benches); fixing anything the gate finds; the deferred Rust reader work
and the Python reader fixes (`docs/ETL-TRADE-STORE-FIXES.md`); ETL adoption; any push to
`upstream`.

## Preconditions

- P1 is done: `target/cpp/wheels/pyarrow-26.0.1+tparquet.1-cp314-*.whl`, the fork venv
  `target/cpp/venv-fork-cp314` (P1 D4) and `target/cpp/build-tpq-query/tpq_query` exist and
  `make -C cpp pyarrow-test tpq-query-test` passes.
- The Massive cache `~/Development/ETL/tmp/massive/trades_v1/` holds `2026-10-26.csv.gz` and
  `2018-06-13.csv.gz`. **Never delete or modify anything in it.**
- `sudo -n zfs`, `sudo -n zpool` work (the harness remounts the bench dataset).

## Memory safety

Same rules as P1 ("Memory safety"): every heavy command under `membound CAP`, exit 137 means
stop and report the step and its peak, never nest `membound`, check
`systemctl show user-1000.slice -p MemoryCurrent` + cap ≤ 190 GiB before each heavy step.
**No table outlives its repetition**: a timed read returns a row count and keeps no reference.

Sizes (2026-11-16 footers): 2026-10-26 has 116,597,573 rows, decoded full table 7.9 GB
(pyarrow), `COLS3` 2.8 GB, Z256k file 1.89 GB, T256k 1.14 GB. 2018-06-13 has 35,710,933
rows, about 0.3× those.

| Task | Step | Estimate | Cap |
|---|---|---|---|
| 1 | harness tests | < 2 GB | `membound 4G` |
| 2 | `make_reference.py 2026-10-26` (Polars reads 2.8 GB gz CSV, transform) | unmeasured; 3–4× the 10 GB frame | `membound 64G` |
| 2 | `make_reference.py 2018-06-13` | about 0.3× | `membound 24G` |
| 3 | each writer child (R memory-mapped 8 GB + encode) | up to 36 GiB measured for the 2026 tparquet write | `membound 48G` |
| 4 | each correctness child (R table + result + oracle) | 8 + 7.9 + 8 = 24 GB for 2026 `full` | `membound 40G` |
| 5, 6 | `run_bench.sh` (one child at a time; worst child `pa_mem` full 2026: 1.9 + 7.9 + 1 GB) | 14 GB + parent 0.5 | `membound 20G` |
| 5, 7 | `gate.py`, `check_results.py`, `summarize.py` | < 0.5 GB | `membound 2G` |

If `make_reference.py` exits 137 at 64G, stop and report: do not raise the cap.

## Conventions for every task

- Paths. `H` = `bench/trade-store/harness`. `DATA` = `~/tmp/tparq-bench/gate` (generated, not in
  the repo): `DATA/<day>.R.arrow`, `DATA/readset/<day>.<fmt>.parquet`, `DATA/logs/`.
  `PY_STOCK` = `target/bench/venv-stock-cp314/bin/python` (Task 1). `PY_FORK` =
  `target/cpp/venv-fork-cp314/bin/python`. `PY_TPQ` = `~/tmp/fmtbench-venv/bin/python` (3.13
  with the tparquet Rust writer; used only by Task 3's reference write). `TPQ_QUERY` =
  `target/cpp/build-tpq-query/tpq_query`.
- Days: `2026-10-26` (gating) and `2018-06-13` (reported, not gating).
- Formats: `Z256k` (zstd, written and read by stock pyarrow), `T256k` and `TS256k` (PCODEC,
  written and read by forked pyarrow and the C++ tool).
- Run every Python script with `-u`. Anything longer than 2 minutes runs in the background:
  `nohup bash -c 'membound CAP CMD; echo "DONE rc=$?"' > DATA/logs/NAME.log 2>&1 &`, waited for
  with Monitor on an until-loop for the `DONE` line. Every Python job prints
  `PEAK_RSS_KIB <ru_maxrss>` as its last line before `DONE`. Read logs with `tail`/`grep`.
- To stop a run use a bracketed pattern, e.g. `pkill -f '[r]eadbench.py'`.
- Never search from `/` or a spinpool mount (`/data`, `/backup`, `/bench/hdd`); never touch
  `/bench/nvme`.
- Before each cold read run (Tasks 5, 6): `sudo -n zpool status spinpool` shows no
  `scrub in progress` or `resilver in progress`, and the second sample of
  `sudo -n zpool iostat spinpool 5 2` shows read bandwidth under 20 MB/s. If not, wait 10
  minutes; after three failures, stop and report.
- Role agents commit their own work on this plan's branch. Generated data and logs under
  `DATA` are never committed. Verifiers check results from files and logs; they never rerun a
  benchmark or a writer.
- Stop and report, never decide, when: a value mismatches, writer parity misses, a coldness
  check fails, a child times out, a cap is hit, or an API this plan names does not exist.

## Decisions (settled; do not revisit)

- **D1. The harness copy.** From `~/Development/ETL/tmp/fmtbench/` copy every `*.py`, `*.sh`,
  `*.json` and `requirements.txt` into `H`, and nothing else (so no `__pycache__/`, `logs/`,
  `*.jsonl`, `summary*.md`, `WHEEL.txt`). Hard-coded paths become environment variables with
  the defaults above: `B` (the script's own directory, already derived in `readbench.py`),
  `DATA` (replaces `TB` in `common.py` and `run_bench.sh`), `PY_STOCK`, `PY_FORK`,
  `TPQ_QUERY`. `H/README.md` records the source path, the copy date, and that the copy is now
  the maintained harness for the format.
- **D2. The ETL snapshot.** Copied from ETL commit `1bed4bbf`
  (`~/Development/ETL/packages/massiveetl/src/massiveetl/equities/us/`): `trade_conditions.py`
  and `trades/{transform,checks,clock,conditions_columnar,enums,log}.py`, into
  `bench/trade-store/etl_snapshot/` with imports rewritten from `massiveetl.equities.us...` to
  the snapshot package. `rilogging` (imported by `log.py` and `conditions_columnar.py`) is
  replaced by a stub module in the snapshot that defines the names those files import as
  no-ops. No other edit. `etl_snapshot/README.md` names the commit and says the snapshot is for
  testing the format; ETL's own tests remain the authority on the transform.
- **D3. The reference table R.** `make_reference.py DAY` (stock venv): `read_source_csv` then
  `transform_day` from the snapshot on `trades_v1/<DAY>.csv.gz`, converted to Arrow
  (`DataFrame.to_arrow()`, `ticker` cast to `pa.string()`), written to
  `DATA/<day>.R.arrow` uncompressed IPC. Its schema must be, in order, all `not null`:
  `ticker: string`, `sip_timestamp: timestamp[ns, tz=UTC]`, `sequence_number`,
  `participant_timestamp`, `trf_timestamp`, `price_e6`, `size_e6`: `int64`, `exchange`, `trf`,
  `tape`, `correction`, `condition_settlement`, `condition_tte_reason`,
  `condition_hours_or_sold`, `condition_trade_detail`: `uint8`, `trade_through_exempt_flag:
  bool`, `unparsed_1`..`unparsed_4`: `uint8` (the 2026-11-16 `Z256k` files' schema); a
  different schema is a stop-and-report. It asserts `ticker` runs are contiguous (the
  transform sorts by its `SORT_KEY`), and prints the row count and the CSV's data-line count.
- **D4. The gate files.** `write_gate_files.py DAY`, one child process per file:
  - `Z256k`, stock venv: the `Z256k` spec of `H/shortlist.json` written by the pyarrow branch
    of `write_spec` in `H/make_formats.py` (zstd 9, 262,144-row groups, the spec's encodings,
    dictionary on the other columns, 1 MiB pages, page index).
  - `T256k_rs`, `PY_TPQ`: the tparquet Rust writer, `policy="auto"`, `level=8`,
    `base="zstd(9)"`, `row_group_size=262144` (the `T256k` spec). Reference for parity only;
    its per-column codecs (`tparquet.ParquetFile(...).metadata.codecs`) are the codec map.
  - `T256k`, fork venv: `pq.write_table(R, row_group_size=262144, compression={c: "pcodec" if
    map[c] == "PCODEC" else "zstd"}, compression_level={c: 8 if pcodec else 9},
    use_dictionary=[non-PCODEC columns], data_page_version="2.0", data_page_size=1 << 20,
    max_rows_per_page=20000, dictionary_pagesize_limit=1 << 20, write_page_index=True,
    write_statistics=True)`. These match arrow-rs's defaults that the Rust writer used (V2
    pages, 1 MiB pages, 20,000-row page limit, 1 MiB dictionary pages, page statistics).
  - `TS256k`, fork venv: the same options, row groups from `g_snapped(ticker_runs(R), 262144)`
    (`H/make_formats.py`) written with `pq.ParquetWriter` one `write_table(slice,
    row_group_size=n)` per group.
- **D5. Writer parity.** Per day: `size(T256k) ≤ 1.01 × size(T256k_rs)`. On a miss, stop and
  report the per-column compressed bytes of both files (`col_bytes` in `H/make_formats.py`).
- **D6. Queries.** `full` (every column), `cols3` (`COLS3` in `H/common.py`:
  `sip_timestamp, price_e6, size_e6`), `ticker1` (the 4th ticker of `tickers_<day>.json`),
  `tickers10` (all ten, one read each, as `readbench.py` does), and `raw` (`os.read` of the
  file, the disk baseline).
- **D7. Readers.**

  | reader | what | formats |
  |---|---|---|
  | `pa` | `pq.read_table(p, columns=..., filters=...)` | all; stock venv for `Z256k`, fork venv otherwise |
  | `pa_nopb` | the same with `pre_buffer=False` | all |
  | `pa_coal` | `ds.dataset(p, format=coal_format())`, as in `readbench.py` | all |
  | `pa_mem` | read the file's bytes, then `pq.read_table(pa.BufferReader(...))`, both timed | all; `full`, `cols3` only |
  | `pa_dec` | the same, the byte read untimed (decode only) | all; `full`, `cols3` only |
  | `cpp`, `cpp_nopb`, `cpp_coal`, `cpp_dec` | `TPQ_QUERY` with the same options (P1 D5: default, `--pre-buffer off`, `--hole-size-limit 4194304 --range-size-limit 67108864`, `--from-memory`) | all (`Z256k` too: the Python-overhead baseline on a standard file); `cpp_dec` `full`, `cols3` only |

  The Polars and tparquet-Rust readers of the old harness are not run (`readers_for` returns
  only the readers above). For `cpp*` readers the child records the tool's own `wall_s`,
  `cpu_s` and `read_bytes`; for `pa*` readers the child records `/proc/self/io` `read_bytes`
  around the timed call besides the existing wall, CPU and disk bytes. Every record carries
  `pa.cpu_count()` and `pa.io_thread_count()` (pa readers) or the tool's thread counts.
  Threads: `OMP_NUM_THREADS=64` as the old harness; the tool gets `--threads 64`.
- **D8. Medians and gate.** Recordsize 4M on `spinpool`, primarycache=metadata, `REPS=5`
  shuffled rounds (the harness's design). Every figure is a median over rounds. The gate on
  2026-10-26, with `M(fmt, query, reader)` the median wall:
  - **G1** `M(T256k, full, pa) ≤ 1.10 × M(Z256k, full, pa)`
  - **G2** `M(T256k, cols3, pa) ≤ 1.10 × M(Z256k, cols3, pa)`
  - **G3** `M(T256k, tickers10, pa) ≤ 1.00 × M(Z256k, tickers10, pa)`
  - **G4** `M(TS256k, full, pa) ≤ 1.10 × M(T256k, full, pa)`
  - **G5** for each query q of full, cols3, ticker1, tickers10 on T256k:
    `|M(T256k, q, pa) − M(T256k, q, cpp)| ≤ 0.10 × M(T256k, q, cpp)`
  - **G6** Task 4 found every comparison equal; **G7** Task 3's parity held.
  2018-06-13 is computed the same way and reported, not gated.
- **D9. Diagnosis, every (day, fmt, query, reader).** Median wall, CPU, disk bytes (from
  `/proc/diskstats`, existing), process `read_bytes`, read amplification = disk bytes ÷ the
  compressed bytes of the column chunks the query needs (from the footer); and for `full` and
  `cols3` the decode-only time (`pa_dec`, `cpp_dec`) and the I/O share = `1 − M(dec) ÷ M(pa)`.

## Task order

1 → 2 → 3 → 4 → 5 → 6 → 7. Each depends on the previous one. After Task 4 the orchestrator
reports the correctness result to the user before the long runs. Runtime: Task 2 about 15 min,
Task 3 about 40 min, Task 4 about 30 min, Task 6 about 2–2.5 h (about 450 cold reads per day, as
the 2026-11-18 run measured 444 reads in 45 min per recordsize and day).

## Task 1 — The harness copy and the stock venv

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/harness/*` (copied per D1), `bench/trade-store/harness/README.md`,
`bench/trade-store/Makefile` (new).

1. Copy per D1 in one commit with no edits ("copy of ETL tmp/fmtbench as of 2026-11-22"), so
   later diffs show every change.
2. `bench/trade-store/Makefile` target `venv-stock`: `uv venv --python 3.14
   target/bench/venv-stock-cp314` then install `pyarrow==26.0.1 polars==2.1.0 numpy==2.5.4
   pytest`. Target `harness-test`: run `H`'s `test_*.py` with `PY_STOCK`.
3. Run `make -C bench/trade-store harness-test` against the verbatim copy and record which
   tests fail on paths (the failing test of this task).
4. Parametrize the paths per D1 until every copied test passes. Tests that exercise the
   Polars or tparquet readers' existence in `readers_for` are updated in Task 5, not here: if
   such a test fails only because of a path, fix the path; do not change reader lists yet.

**Done when:** `membound 4G make -C bench/trade-store harness-test` passes; `grep -rn
'/home/ashwin' bench/trade-store/harness` finds only defaults read through `os.environ.get` /
`${VAR:-...}`; the first commit of the task is the verbatim copy.

## Task 2 — The ETL snapshot and the reference table R

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/etl_snapshot/` (D2), `bench/trade-store/harness/make_reference.py`,
`bench/trade-store/harness/test_make_reference.py`.

1. Test first (`test_make_reference.py`, stock venv): on the first 50,000 data lines of
   `trades_v1/2018-06-13.csv.gz` (written gzip'd to a pytest `tmp_path`, header kept), the
   snapshot's `transform_day(read_source_csv(...))` equals ETL's own output for the same
   slice. ETL's output is produced once by running ETL's venv
   (`~/Development/ETL/.venv/bin/python`, read-only use, `membound 8G`) on the slice and saving
   IPC to `tmp_path`; the test compares with `pa.Table.equals`. A second test runs
   `make_reference.py` on the slice and checks the IPC file's schema, row count, and that
   `ticker` runs are contiguous. Both fail first (no snapshot, no script).
2. Copy the snapshot per D2; write `make_reference.py` per D3.
3. Run it for 2026-10-26 (`membound 64G`) and 2018-06-13 (`membound 24G`) in the background.

**Done when:** both tests pass; `DATA/2026-10-26.R.arrow` has 116,597,573 rows and
`DATA/2018-06-13.R.arrow` 35,710,933 rows (the row counts of the 2026-11-16 bench files; a
different count is a stop-and-report, not a fix), each log ends `DONE rc=0` with its
`PEAK_RSS_KIB`, which the task report states.

## Task 3 — Gate files and writer parity

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/harness/write_gate_files.py`,
`bench/trade-store/harness/test_write_gate_files.py`.

1. Test first, on a 200,000-row slice of `DATA/2018-06-13.R.arrow` (first rows, so whole ticker
   runs): `write_gate_files.py --day D --rows 200000 --out tmp_path` writes the four files of D4;
   the test checks that `T256k`'s footer has PCODEC on exactly the columns `T256k_rs` has it on,
   V2 data pages, no dictionary on PCODEC columns, 262,144-row groups (one, at this size), a page
   index; that `TS256k`'s row groups end on ticker boundaries; and that the script prints a
   `PARITY` line with both sizes. (Footer facts come from `pq.ParquetFile(...).metadata` in the
   fork venv: the test runs in `PY_FORK`.)
2. Implement per D4 and D5, one child process per file, each in its own interpreter, under
   `membound 48G`; a child whose `membound` exits 137 stops the run.
3. Run for both days in the background. Print, per day: the four sizes, the codec map, and
   `PARITY day ratio=<T256k/T256k_rs> PASS|FAIL`.

**Done when:** the test passes; `DATA/readset/<day>.{Z256k,T256k,TS256k}.parquet` exist for both
days; both `PARITY` lines say `PASS` (a `FAIL` is a stop-and-report with the per-column bytes,
D5); `T256k_rs` files are deleted after the run (parity is in the log).

## Task 4 — Three-way correctness

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/harness/check_gate.py`, `bench/trade-store/harness/test_check_gate.py`.

`check_gate.py --day D` runs, as separate child processes under `membound 40G`, one per row:

| reader | interpreter | file | compared to |
|---|---|---|---|
| stock pyarrow | `PY_STOCK` | `Z256k` | R |
| forked pyarrow | `PY_FORK` | `T256k`, `TS256k` | R |
| C++ | `TPQ_QUERY --compare-to DATA/<day>.R.arrow --oracle-format ipc` | `T256k`, `TS256k`, `Z256k` | R |

for each query of D6 except `raw` (`tickers10` checks each ticker). Python children compute the
query on R with the same columns and `pc.field("ticker")` filter (`pa.dataset` over the IPC
file), compare with `pa.Table.equals`, and print one JSON line per comparison. Exit 1 on any
mismatch, after printing the first differing column and row.

1. Test first: on Task 3's small files (regenerated into `tmp_path` by calling
   `write_gate_files.py`), every comparison is equal; with one `price_e6` value changed in a
   copy of R, the checker exits 1 and names `price_e6`. Fails first (no script).
2. Implement; run both days in the background.

**Done when:** the test passes; for both days every comparison line reads `"equal": true` (24
per day: 4 queries × (1 + 2 + 3) reader-file pairs, `tickers10` counted once); the log ends
`DONE rc=0`. **Then the orchestrator reports to the user before Task 5.**

## Task 5 — Harness readers, diagnosis fields, gate script; smoke

- **Tier** — opus + medium (T4)

Files: `H/readbench.py`, `H/run_bench.sh`, `H/common.py`, `H/gate.py` (new), `H/test_gate.py`
(new), the harness tests that cover `readers_for` and `expected_combos`.

1. Test first:
   - `readers_for` and `expected_combos` return exactly D7's readers per format and query;
     the child for `Z256k` runs `PY_STOCK` and for `T256k`/`TS256k` runs `PY_FORK`; `cpp*`
     readers build the `TPQ_QUERY` argument lists of D7.
   - `test_gate.py`: `gate.py` on a synthetic `results.jsonl` (written by the test) with known
     medians prints each of G1–G5 with its measured ratio and `PASS`/`FAIL`, and the D9 table;
     one case where G3 is exactly 1.00 passes, one where it is 1.01 fails.
   Run and see them fail.
2. `readbench.py`: D7 readers; the interpreter chosen per format; `/proc/self/io` and thread
   counts in each record. `run_bench.sh`: formats from `FMTS` (default `Z256k T256k TS256k`),
   files from `DATA/readset`, parent run with `PY_STOCK`. `gate.py`: D8 and D9 from
   `results.jsonl`, writing `DATA/gate-<label>.md`.
3. Smoke (pre-run checks first): `DAYS=2018-06-13 REPS=1 QUERIES=ticker1,cols3
   membound 20G H/run_bench.sh hdd 4M` with label suffix `-smoke`; then `check_results.py` for
   that label.

**Done when:** the tests pass; the smoke log ends `DONE rc=0`; `check_results.py` prints `OK`;
`results.jsonl` holds a row for every (fmt, query, reader) of D7 for that day and those
queries, each with `read_bytes` and thread counts; `zfs get recordsize,primarycache` on the
bench dataset reads `1M`, `metadata` afterwards.

## Task 6 — The cold run

- **Tier** — sonnet + medium (T2)

1. Pre-run checks (Conventions).
2. `REPS=5 membound 20G H/run_bench.sh hdd 4M` in the background (both days, all D6 queries,
   all D7 readers); about 2–2.5 h.
3. `membound 2G PY_STOCK -u H/check_results.py hdd-4M`.

**Done when:** the log ends `DONE rc=0`; `check_results.py hdd-4M` prints `OK`; every
(day, fmt, query, reader) of D7 has 5 rounds; no row has `settle_timeout: true` (if any does,
stop and report).

## Task 7 — The gate report

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/GATE-RESULTS.md` (new), `docs/ETL-TRADE-STORE-FIXES.md`.

1. `membound 2G PY_STOCK -u H/gate.py hdd-4M`.
2. `GATE-RESULTS.md`: the date, the commit, the wheel version, the setup (rey, spinpool, 4M,
   primarycache=metadata, REPS=5); G1–G7 with ratios and PASS/FAIL; the D9 diagnosis table for
   both days; Task 3's sizes and parity; Task 4's result; the TS256k-vs-T256k finding (does the
   2.4× of the Rust reader persist?). Numbers only from `gate.py`'s output and the logs; no
   recommendation beyond stating which gates failed and which D9 figures bear on each.
3. In `docs/ETL-TRADE-STORE-FIXES.md`, replace the "Work, in order" steps this work completed
   with one line pointing at `GATE-RESULTS.md` and its overall PASS/FAIL.

**Done when:** every number in `GATE-RESULTS.md` traces to `gate.py`'s output or a named log
line; the overall verdict is PASS only if G1–G7 all pass. **Then stop and report to the user**,
whatever the verdict.
