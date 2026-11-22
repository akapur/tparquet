# P1 — Arrow C++ fork with the Python components, and a forked pyarrow wheel

## Goal and scope

Build the Arrow C++ 26.0.1 fork (`cpp/arrow`) with the components pyarrow needs, teach
pyarrow's Cython layer the PCODEC codec, and build a pyarrow 26.0.1 wheel against the fork on
rey for CPython 3.14. Add the C++ query tool that P2's gate benchmark and correctness check call.
This is the first of two plans; `docs/plans/2026-11-22-P2-trade-store-gate.md` uses what this
one builds.

Why (settled with the user on 2026-11-22, `docs/ETL-TRADE-STORE-FIXES.md`): C++ and Python read
and write PCODEC through one engine, Arrow C++; Python is pyarrow, a thin wrapper. Rust stays
only inside the codec (`rust/core` via `rust/capi`, linked into `libarrow.so`).

**In scope:** a `python` variant of `cpp/arrow/build.sh`; a pyarrow wheel built from the fork's
source tree with a make target that takes the interpreter as a parameter; the PCODEC patch to
pyarrow's Cython (part of `FORK.patch`, listed in `FORK.md`, checked by
`scripts/check-cpp-fork.sh`); tests of that patch; the C++ query tool
`bench/trade-store/cpp/tpq_query`.

**Out of scope:** portable (manylinux) wheels, hosts other than rey, how ETL pins the wheel
(ETL decides that when it adopts it), deleting the tparquet Rust reader or writer, the
coalesced-reads and polars fixes (deferred: they matter only if P2's gate fails), any push to
`upstream`.

## Preconditions (the user, before Task 1)

The fork has never been built on rey (`target/cpp` holds only the tarball). These Ubuntu 26.04
packages are missing and need root:

```
sudo apt install libsnappy-dev liblz4-dev libbrotli-dev libthrift-dev libre2-dev \
  libutf8proc-dev rapidjson-dev
```

Task 1 stops and reports if CMake names another missing package; it never installs packages.

## Memory safety

rey: 251 GiB, no swap; all of the user's sessions together are capped at 200G
(`~/.claude/CLAUDE.md`, "Memory").

- Every build, test or data command runs under `membound CAP ...` (`~/.local/bin/membound`)
  with the cap below. Exit 137 means the job went over its cap: stop and report; never just
  raise the cap. Never nest `membound`.
- Before each heavy step: `systemctl show user-1000.slice -p MemoryCurrent` plus the step's cap
  is at most 190 GiB. If not, wait 10 minutes and check again; after three failures, stop and
  report.

| Step | Estimate | Cap |
|---|---|---|
| Arrow `python` variant build, `JOBS=24` | 24 g++ jobs × ~2 GB | `membound 64G` |
| pyarrow wheel build, `CMAKE_BUILD_PARALLEL_LEVEL=24` | Cython-generated C++ is large: 24 × ~2.5 GB | `membound 64G` |
| `cargo build --release -p tparquet-capi` | < 8 GB | `membound 16G` |
| pytest of the patch, `tpq_query` tests (synthetic data, < 1M rows) | < 4 GB | `membound 8G` |

## Conventions for every task

- Work on branch `worktree-pyarrow-fork-gate` (this plan's branch). Role agents commit their
  own work. Never push to `upstream` (memory: never push upstream).
- Run anything longer than 2 minutes in the background:
  `nohup bash -c 'membound CAP CMD; echo "DONE rc=$?"' > target/cpp/logs/NAME.log 2>&1 &`,
  and wait for the `DONE` line with Monitor on an until-loop. Read logs with `tail` or `grep`,
  never in full.
- Never search from `/` or any spinpool mount (`/data`, `/backup`, `/bench/hdd`): it thrashes
  the HDD array. Search only inside the repo or a path this plan names.
- TDD (AGENTS.md): each commit message says which test failed first, or why no test could.
- Stop and report, never decide, when: a package is missing, an Arrow or pyarrow API this
  plan names does not exist, a cap is hit, or a test fails for a reason the task does not
  describe.

## Decisions (settled; do not revisit)

- **D1. The build variant.** `cpp/arrow/build.sh python` is `release` plus `-DARROW_COMPUTE=ON
  -DARROW_CSV=ON -DARROW_JSON=ON -DARROW_DATASET=ON -DARROW_ACERO=ON -DARROW_FILESYSTEM=ON
  -DARROW_JEMALLOC=ON -DARROW_WITH_RE2=ON -DARROW_WITH_UTF8PROC=ON`, installed in
  `target/cpp/arrow-python/install`. jemalloc because stock pyarrow wheels default to it on
  Linux: P2 compares against stock pyarrow, so the allocator must match. jemalloc is fetched by
  Arrow's own bundled-dependency mechanism (version and checksum pinned by Arrow); every other
  dependency stays `SYSTEM`. The `release` and `asan` variants are unchanged, so `make -C cpp
  build test` and `cpp/mutation` are unaffected.
- **D2. The pyarrow patch.** Is part of `cpp/arrow/FORK.patch`. `check-cpp-fork.sh` extracts
  and diffs `cpp/` **and** `python/pyarrow/` (today it handles `cpp/` only), and `FORK.md`'s
  table lists each pyarrow file. Changes, and nothing else:
  - `python/pyarrow/includes/libparquet.pxd`: `ParquetCompression_PCODEC" parquet::Compression::PCODEC"`
    in the enum that holds `ParquetCompression_ZSTD" parquet::Compression::ZSTD"`.
  - `python/pyarrow/includes/libarrow.pxd`: `CCompressionType_PCODEC" arrow::Compression::PCODEC"`
    in the enum that holds `CCompressionType_LZ4_FRAME" arrow::Compression::LZ4_FRAME"`.
  - `python/pyarrow/_parquet.pyx`: a `ParquetCompression_PCODEC: 'PCODEC'` entry in
    `cdef compression_name_from_enum`; `'PCODEC'` in the set of `cdef int check_compression_name`;
    an `elif name == 'PCODEC'` arm in `cdef ParquetCompression compression_from_name`.
  - `python/pyarrow/io.pxi`: an `elif uppercase == 'PCODEC'` arm in
    `cdef CCompressionType _ensure_compression`, so `pa.Codec("pcodec")` and
    `pa.Codec.is_available("pcodec")` work.
  - The docstrings that list valid compression values (`_parquet.pyx` "One of 'UNCOMPRESSED',
    'SNAPPY', 'GZIP', 'LZO', 'BROTLI', 'LZ4', 'ZSTD',"; `parquet/core.py` "Valid values: {'NONE',
    'SNAPPY', 'GZIP', 'BROTLI', 'LZ4', 'LZ4_RAW', 'ZSTD'}.") gain `'PCODEC'`.
- **D3. The wheel build.** Never in `target/cpp/arrow-src`, which must stay a clean patched
  tree for `check-cpp-fork.sh --update`. `cpp/arrow/build-pyarrow.sh PYTHON` copies
  `target/cpp/arrow-src/python` to a fresh `target/cpp/pyarrow-build/<python-tag>/src` (`<python-tag>` is `cp` plus the interpreter's
  major and minor version, e.g. `cp314`), and runs
  `uv build --wheel --python PYTHON` there with `ARROW_HOME` and `CMAKE_PREFIX_PATH` set to
  `target/cpp/arrow-python/install`, `PYARROW_BUNDLE_ARROW_CPP=ON` (no `LD_LIBRARY_PATH`
  needed), `PYARROW_WITH_PARQUET=1`, `PYARROW_WITH_DATASET=1`, `PYARROW_WITH_ACERO=1`, and
  `SETUPTOOLS_SCM_PRETEND_VERSION=26.0.1+tparquet.1`. The wheel goes to
  `target/cpp/wheels/`. The make target is `make -C cpp pyarrow PYTHON=...`; `PYTHON`
  defaults to `$(uv python find 3.14)`. The local version `+tparquet.N` is bumped by hand when the
  patch changes.
- **D4. The fork venv.** `target/cpp/venv-fork-<python-tag>`: the wheel plus `pytest`,
  `numpy==2.5.4`, `polars==2.1.0`, created by `make -C cpp pyarrow-venv PYTHON=...`. P2 uses
  it.
- **D5. The C++ query tool.** `bench/trade-store/cpp/` is its own CMake project
  (`find_package(ArrowDataset)`, `find_package(Parquet)` from
  `target/cpp/arrow-python/install`); binary `tpq_query`. It is a thin CLI over
  `arrow::dataset`: no query logic outside Arrow, so that it runs the same C++ code path as
  pyarrow's `pq.read_table(filters=...)` and `ds.dataset(...).to_table(...)`. Interface:

  ```
  tpq_query --file F [--format parquet|ipc] [--columns c1,c2,...] [--ticker T]...
            [--pre-buffer on|off] [--hole-size-limit BYTES --range-size-limit BYTES]
            [--from-memory] [--threads N] [--compare-to ORACLE --oracle-format ipc|parquet]
  ```

  - `--ticker` repeated: the filter is `ticker == T` for one, `is_in(ticker, [...])` for
    several; no `--ticker`: no filter. `--columns` absent: every column.
  - `--pre-buffer` defaults to `on`, as `pq.read_table`. The hole/range limits map to
    `arrow::io::CacheOptions`; when given, `lazy=false`, as P2's `pa_coal` reader.
  - `--from-memory`: read the whole file into a buffer first (not timed), then scan an
    `arrow::io::BufferReader`; the decode-only measurement of P2.
  - `--threads N`: `arrow::SetCpuThreadPoolCapacity(N)`; default 64.
  - Output: one JSON line on stdout: `{"rows": R, "wall_s": W, "cpu_s": C, "read_bytes": B,
    "cpu_threads": N, "io_threads": M}`. `wall_s` and `cpu_s` time the scan only
    (`std::chrono::steady_clock`, `getrusage(RUSAGE_SELF)`); `read_bytes` is the difference of
    `read_bytes` in `/proc/self/io` around the scan.
  - `--compare-to ORACLE`: run the same query (columns and filter) on ORACLE as well and
    compare the two tables with `arrow::Table::Equals` (chunk layout does not matter to it).
    On a mismatch, print the first differing column and row to stderr and exit 1. Adds
    `"equal": true` to the JSON line.
  - Exit 2 on a usage error, 1 on any Arrow error (message on stderr).

## Task order

1 → 2 → 3 → 4. Each depends on the previous one.

## Task 1 — The `python` build variant

- **Tier** — sonnet + medium (T2)

Files: `cpp/arrow/build.sh`, `cpp/Makefile`, `cpp/arrow/FORK.md` (the `build.sh` bullet).

1. Add the `python` variant to `build.sh` per D1: the `case "$variant"` arm and the component
   flags (the shared `cmake` line keeps `release`'s `OFF` flags; the `python` arm appends the
   `ON` flags after them, as later definitions win). Update the usage comment at the top and
   the `unknown variant` message.
2. `cpp/Makefile`: target `arrow-python: rust` running `JOBS=$(JOBS) arrow/build.sh python`;
   add it to `.PHONY` and the usage comment.
3. Build: `JOBS=24 membound 64G make -C cpp arrow-python` in the background (Conventions).
   Expect 20–40 min the first time.

No failing test can be written first: this is build configuration. The commit message says
so; the check below is the test.

**Done when:** the log ends `DONE rc=0`; `target/cpp/arrow-python/install/lib` contains
`libarrow.so`, `libarrow_compute.so`, `libarrow_acero.so`, `libarrow_dataset.so` and
`libparquet.so`; `grep -c ARROW_WITH_PCODEC target/cpp/arrow-python/install/include/arrow/util/config.h`
is ≥ 1 and `grep ARROW_JEMALLOC target/cpp/arrow-python/install/include/arrow/util/config.h`
shows it defined; `make -C cpp build test` still passes (`membound 32G`).

## Task 2 — The wheel, unpatched Cython: read PCODEC from Python

- **Tier** — opus + medium (T4)

Files: `cpp/arrow/build-pyarrow.sh` (new), `cpp/Makefile`, `cpp/arrow/FORK.md` (a bullet for
`build-pyarrow.sh` next to `build.sh`'s), `cpp/arrow/python-tests/test_read.py` (new),
`cpp/arrow/python-tests/conftest.py` (new).

Reading needs no Cython change: the C++ reader finds the codec. This task proves that before
the patch exists.

1. Test first: `test_read.py` reads every PCODEC file of `testdata/files` with
   `pq.read_table` and checks its canonical JSON lines equal the vector's expected lines in
   `testdata/expected` (see `testdata/README.md` for the pairing). Canonical JSON comes from
   `python/src/tparquet/canonical.py`, loaded by file path with `importlib.util` (importing the
   `tparquet` package would load its Rust extension, which this venv must not need). A second
   test asserts `pq.ParquetFile(f).metadata.row_group(0).column(i).compression` does not raise
   for a PCODEC column (it returns `'UNKNOWN'` until Task 3). Run it under stock pyarrow
   26.0.1 (`uv run --with pyarrow==26.0.1 --with pytest pytest cpp/arrow/python-tests`) and
   see it fail: stock pyarrow rejects codec 20547 (it aborts the process with
   `Unsupported compression codec value: 20547`; a test that aborts counts as failing, record
   the message).
2. `build-pyarrow.sh` and the make targets `pyarrow` and `pyarrow-venv` per D3 and D4.
   `pyarrow` depends on `arrow-python`.
3. Build: `membound 64G make -C cpp pyarrow` in the background, then
   `membound 8G make -C cpp pyarrow-venv`.
4. Run the tests in the fork venv: `membound 8G target/cpp/venv-fork-<tag>/bin/python -m pytest
   cpp/arrow/python-tests`. Add a make target `pyarrow-test` that does this.

**Done when:** `target/cpp/wheels/pyarrow-26.0.1+tparquet.1-cp314-*.whl` exists; in the fork
venv `python -c "import pyarrow, pyarrow.dataset, pyarrow.parquet; print(pyarrow.__version__)"`
prints `26.0.1+tparquet.1` with `LD_LIBRARY_PATH` unset; `ldd` of the venv's
`pyarrow/lib*.so` resolves `libarrow.so` inside the venv; `make -C cpp pyarrow-test` passes;
`git status` shows `target/cpp/arrow-src` unchanged (`scripts/check-cpp-fork.sh` passes).

## Task 3 — The Cython patch: write PCODEC from Python

- **Tier** — opus + medium (T4)

Files: `cpp/arrow/python-tests/test_write.py` (new), `cpp/arrow/FORK.patch`,
`cpp/arrow/FORK.md`, `scripts/check-cpp-fork.sh`, `cpp/arrow/python-tests/test_read.py`.

1. Test first, `test_write.py`, each run in the fork venv from Task 2 and seen failing before
   the patch:
   - `pq.write_table(t, f, compression={"a": "pcodec", "s": "zstd"}, use_dictionary=["s"],
     data_page_version="2.0")` for a table with an `int64` column `a`, a `double` column `d`, a
     `timestamp[ns, tz=UTC]` column `ts` and a string column `s` (100,000 rows, seeded random);
     reading it back gives an equal table; `metadata.row_group(0).column(0).compression ==
     "PCODEC"`; the footer has `parquet.pcodec.version` = `"1"`.
   - `compression="pcodec"` on every column (the string column too) round-trips.
   - `compression_level={"a": 12}` round-trips and `compression_level={"a": 13}` raises
     `pa.ArrowInvalid` (levels 0..12, `FORK.md` `compression_pcodec.cc`).
   - `pa.Codec.is_available("pcodec")` is `True`; `pa.Codec("pcodec").name == "pcodec"`.
   - An unknown name still raises (`compression="nope"` → `ArrowException`, upstream
     behaviour kept).
   (Reading these files from C++ is Task 4's test; stock pyarrow on them is documented in
   `FORK.md`, not tested, because it aborts the process.)
   Change Task 2's metadata test to expect `"PCODEC"`.
2. `check-cpp-fork.sh`: extract `apache-arrow-${version}/python` besides `.../cpp`; in
   `--update`, copy `$src/python/pyarrow` to `fork/python/pyarrow` and diff `upstream` vs
   `fork` over both `cpp` and `python/pyarrow`. Files that `uv build` would generate never
   reach `arrow-src` (D3), so the diff holds only source changes.
3. Make the D2 changes in `target/cpp/arrow-src`, run `scripts/check-cpp-fork.sh --update`,
   then list each pyarrow file in `FORK.md`'s "Changes" table (Item, Change, Why) and update
   the summary line ("the complete diff of `cpp/`, 15 files") to name both trees and the new
   count. Add a `FORK.md` paragraph: stock pyarrow aborts the process on a PCODEC file
   (`terminate called ... Unsupported compression codec value: 20547`); a program that may run
   on stock pyarrow checks `pa.Codec.is_available("pcodec")` first.
4. Rebuild the wheel (`make -C cpp pyarrow` re-copies the source per D3) and the venv; run
   `make -C cpp pyarrow-test`.

**Done when:** `scripts/check-cpp-fork.sh` passes and reports the new file count;
`make -C cpp pyarrow-test` passes with every test above; `make -C cpp build test` passes;
`git diff --stat` touches only the files listed for this task.

## Task 4 — The C++ query tool `tpq_query`

- **Tier** — opus + medium (T4)

Files: `bench/trade-store/cpp/CMakeLists.txt`, `bench/trade-store/cpp/tpq_query.cc`,
`bench/trade-store/cpp/test_tpq_query.py`, `bench/trade-store/cpp/README.md`, `cpp/Makefile`
(targets `tpq-query`, `tpq-query-test`).

1. Test first: `test_tpq_query.py` (pytest, fork venv) builds a 300,000-row table shaped like
   the trade store (`ticker` string sorted, 30 tickers; `sip_timestamp` timestamp[ns, UTC];
   `price_e6`, `size_e6`, `sequence_number` int64; `exchange` uint8) and writes it twice with
   forked pyarrow: Parquet with PCODEC on the int64 columns and 65,536-row row groups, and
   Arrow IPC (the oracle). It then runs the binary and checks, for each of: no options;
   `--columns sip_timestamp,price_e6,size_e6`; one `--ticker`; ten `--ticker`;
   `--pre-buffer off`; `--hole-size-limit 4194304 --range-size-limit 67108864`;
   `--from-memory`:
   - `rows` equals the row count pyarrow gets for the same query (`pq.read_table(f,
     columns=..., filters=...)`);
   - with `--compare-to` the IPC file, exit 0 and `"equal": true`;
   - a corrupted oracle (one `price_e6` value changed) gives exit 1 and names `price_e6` on
     stderr;
   - a missing `--file` gives exit 2.
   Run it against a stub `tpq_query` that prints `{}` and see it fail.
2. Implement per D5. Build with `cmake -S bench/trade-store/cpp -B target/cpp/build-tpq-query
   -G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_PREFIX_PATH=target/cpp/arrow-python/install`.
   The `tpq-query` make target does that; `tpq-query-test` runs the pytest file with
   `TPQ_QUERY=target/cpp/build-tpq-query/tpq_query`.
3. `README.md`: the interface of D5, how to build, and that it deliberately holds no query
   logic outside `arrow::dataset`.

**Done when:** `membound 8G make -C cpp tpq-query-test` passes; `tpq_query --help` exits 0;
the binary's `ldd` shows the fork's `libarrow_dataset.so` from
`target/cpp/arrow-python/install`.
