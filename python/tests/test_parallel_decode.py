"""Row groups decoded on worker threads (`threads`): the batches of one thread, in the order
asked, with at most `threads` row groups decoded at once; errors where one thread meets them;
nothing left behind when a stream is dropped."""

from __future__ import annotations

import gc
import io
import os
import pathlib
from collections.abc import Callable
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import tparquet as ppc
from tparquet import _lib

TABLE = pa.table(
    {
        "id": pa.array(range(1000), pa.int64()),
        "x": pa.array([i / 3 for i in range(1000)], pa.float32()),
        "s": pa.array([f"v{i % 11}" for i in range(1000)]),
        "same": pa.array([f"k{i % 5}" for i in range(1000)]).dictionary_encode(),
        # values of each row group of its own, so dictionaries differ between row groups
        "own": pa.array([f"g{i // 300}-{i % 3}" for i in range(1000)]).dictionary_encode(),
        "nested": pa.array([{"a": i, "b": [i, i]} for i in range(1000)]),
    }
)
THREADS = [2, 3, 8, None]


@pytest.fixture(scope="module")
def path(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    p = tmp_path_factory.mktemp("parallel") / "t.parquet"
    ppc.write_table(TABLE, p, policy="pcodec", row_group_size=300)
    return p


def batches(f: ppc.ParquetFile, **kwargs: Any) -> list[pa.RecordBatch]:
    return [pa.record_batch(b) for b in f.stream(**kwargs)]


def assert_same(got: list[pa.RecordBatch], want: list[pa.RecordBatch]) -> None:
    assert [b.num_rows for b in got] == [b.num_rows for b in want]
    for g, w in zip(got, want, strict=True):
        assert g.schema == w.schema
        assert g.equals(w)
        for gc_, wc in zip(g.columns, w.columns, strict=True):
            if isinstance(wc, pa.DictionaryArray):
                assert isinstance(gc_, pa.DictionaryArray)
                assert gc_.dictionary.equals(wc.dictionary)
                assert gc_.indices.equals(wc.indices)


def sources(path: pathlib.Path) -> list[ppc.ParquetFile]:
    return [ppc.ParquetFile(path), ppc.ParquetFile(io.BytesIO(path.read_bytes()))]


@pytest.mark.parametrize("threads", THREADS)
@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"columns": ["s", "id"]},
        {"columns": ["own", "same"]},
        {"columns": []},
        {"row_groups": [2, 0, 0, 3]},
        {"row_groups": [3, 1], "columns": ["own"]},
        {"row_groups": []},
        {"limit": 0},
        {"limit": 1},
        {"limit": 450},
        {"limit": 600},
        {"limit": 999},
        {"limit": 5000},
        {"limit": 350, "row_groups": [2, 0, 0, 3]},
        {"limit": 10, "columns": []},
    ],
)
@pytest.mark.parametrize("batch_size", [1, 7, 299, 300, 301, 2**40])
def test_threads_give_the_batches_of_one_thread(
    path: pathlib.Path, threads: int | None, kwargs: dict[str, Any], batch_size: int
) -> None:
    for f in sources(path):
        want = batches(f, threads=1, batch_size=batch_size, **kwargs)
        assert_same(batches(f, threads=threads, batch_size=batch_size, **kwargs), want)


def test_one_thread_reads_as_before(path: pathlib.Path) -> None:
    f = ppc.ParquetFile(path)
    assert_same(batches(f, threads=1), batches(f))
    assert pa.Table.from_batches(batches(f, threads=1)).equals(TABLE)
    assert [b.num_rows for b in batches(f, threads=1, batch_size=400)] == [400, 400, 200]


@pytest.mark.parametrize("threads", [1, 2])
def test_every_reader_takes_threads(path: pathlib.Path, threads: int) -> None:
    import duckdb

    import tparquet.duckdb as pdd
    import tparquet.pandas as ppd
    import tparquet.polars as ppl

    ids = list(range(1000))
    f = ppc.ParquetFile(path)
    con = duckdb.connect()
    reads: list[Callable[[], object]] = [
        lambda: f.read(threads=threads).equals(TABLE),
        lambda: pa.Table.from_batches(list(f.iter_batches(threads=threads))).equals(TABLE),
        lambda: ppc.read_table(path, threads=threads).equals(TABLE),
        lambda: pa.RecordBatchReader.from_stream(ppc.read_batches(path, threads=threads)).read_all().equals(TABLE),
        lambda: ppd.read_parquet(path, columns=["id"], threads=threads)["id"].tolist() == ids,
        lambda: ppl.read_parquet(path, columns=["id"], threads=threads)["id"].to_list() == ids,
        lambda: ppl.scan_parquet(path, threads=threads).select("id").collect()["id"].to_list() == ids,
        lambda: (
            sorted(r[0] for r in pdd.read_parquet(path, connection=con, columns=["id"], threads=threads).fetchall())
            == ids
        ),
        lambda: (
            pdd.sql_view(con, "v", path, columns=["id"], threads=threads).execute("select sum(id) from v").fetchone()
            == (sum(ids),)
        ),
    ]
    for read in reads:
        before = _lib._decode_threads_started()
        assert read() is True
        # a dropped `threads` would read with the default, one thread per CPU
        assert (_lib._decode_threads_started() > before) == (threads > 1)


@pytest.mark.parametrize("bad", [0, -1, -(2**63)])
def test_threads_must_be_positive(path: pathlib.Path, bad: int) -> None:
    import tparquet.polars as ppl

    f = ppc.ParquetFile(path)
    calls: list[Callable[[], object]] = [
        lambda: f.stream(threads=bad),
        lambda: f.read(threads=bad),
        lambda: ppc.read_table(path, threads=bad),
    ]
    for read in calls:
        with pytest.raises(ppc.PcodecError, match=rf"^threads must be positive, got {bad}$"):
            read()
    with pytest.raises(ValueError, match=rf"^threads must be positive, got {bad}$"):
        ppl.scan_parquet(path, threads=bad)


@pytest.mark.parametrize("bad", [True, 2.0, "2"])
def test_threads_must_be_ints(path: pathlib.Path, bad: object) -> None:
    f = ppc.ParquetFile(path)
    with pytest.raises(TypeError, match=rf"^threads must be an int, got {type(bad).__name__}$"):
        f.stream(threads=bad)  # type: ignore[arg-type]
    with pytest.raises(ppc.PcodecError, match=rf"^threads {2**63} is out of range$"):
        f.stream(threads=2**63)


def corrupt_row_group(path: pathlib.Path, out: pathlib.Path, rg: int) -> None:
    data = bytearray(path.read_bytes())
    start = pq.ParquetFile(path).metadata.row_group(rg).column(0).data_page_offset
    data[start : start + 16] = b"\xff" * 16
    out.write_bytes(bytes(data))


def read_until_error(f: ppc.ParquetFile, **kwargs: Any) -> tuple[list[pa.RecordBatch], str]:
    got = []
    with pytest.raises(ppc.PcodecError) as error:  # noqa: PT012
        for b in f.stream(**kwargs):
            got.append(pa.record_batch(b))
    return got, str(error.value)


@pytest.mark.parametrize("threads", THREADS)
def test_an_error_comes_after_the_row_groups_before_it(
    path: pathlib.Path, tmp_path: pathlib.Path, threads: int | None
) -> None:
    bad = tmp_path / "bad.parquet"
    corrupt_row_group(path, bad, 2)
    for f in sources(bad):
        cases: list[dict[str, Any]] = [
            {"batch_size": 100},
            {"batch_size": 100, "row_groups": [3, 0, 2, 1]},
            {"batch_size": 1000},
        ]
        for kwargs in cases:
            want, error = read_until_error(f, threads=1, **kwargs)
            got, got_error = read_until_error(f, threads=threads, **kwargs)
            assert_same(got, want)
            assert got_error == error
        assert [b.num_rows for b in read_until_error(f, batch_size=100, threads=threads)[0]] == [100] * 6
        # a limit that ends before the damaged row group never reads it
        assert sum(b.num_rows for b in f.stream(limit=600, threads=threads)) == 600


@pytest.fixture(scope="module")
def uneven(tmp_path_factory: pytest.TempPathFactory) -> pathlib.Path:
    """Three row groups that take a while to decode and a last one of one row."""
    p = tmp_path_factory.mktemp("uneven") / "u.parquet"
    rng = np.random.default_rng(0)
    n = 3 * 400_000 + 1
    pq.write_table(pa.table({c: pa.array(rng.integers(0, 2**40, n)) for c in "abcd"}), p, row_group_size=400_000)
    return p


def threads_now() -> int:
    return len(os.listdir("/proc/self/task"))


@pytest.mark.parametrize("threads", [1, 2, 3, 4, 8])
def test_at_most_threads_row_groups_are_decoded_at_once(path: pathlib.Path, threads: int) -> None:
    f = ppc.ParquetFile(path)
    before = _lib._decode_threads_started()
    stream = iter(f.stream(batch_size=300, threads=threads))
    assert _lib._decode_threads_started() == before  # none before the first batch is asked for
    started = []
    for _ in stream:
        started.append(_lib._decode_threads_started() - before)
    # each batch is one row group: a row group is started only once the one before it in
    # `threads` is taken
    if threads == 1:
        assert started == [0, 0, 0, 0]
    else:
        assert started == [min(threads + i, 4) for i in range(4)]


@pytest.mark.skipif(not os.path.isdir("/proc/self/task"), reason="needs /proc")
@pytest.mark.parametrize("threads", [1, 4, None])
def test_a_dropped_stream_stops_its_threads(uneven: pathlib.Path, threads: int | None) -> None:
    f = ppc.ParquetFile(uneven)
    before = threads_now()
    stream = iter(f.stream(row_groups=[3, 0, 1, 2], batch_size=1, threads=threads))
    next(stream)
    del stream
    gc.collect()
    assert threads_now() == before


@pytest.mark.skipif(not os.path.isdir("/proc/self/fd"), reason="needs /proc")
@pytest.mark.parametrize("threads", [4, None])
def test_dropping_a_half_read_stream_closes_the_file(uneven: pathlib.Path, threads: int | None) -> None:
    gc.collect()
    before = len(os.listdir("/proc/self/fd")), threads_now()
    f = ppc.ParquetFile(uneven)
    stream = iter(f.stream(row_groups=[3, 0, 1, 2], batch_size=1, threads=threads))
    next(stream)
    del f, stream
    gc.collect()
    assert (len(os.listdir("/proc/self/fd")), threads_now()) == before


@pytest.mark.skipif(not os.path.isdir("/proc/self/task"), reason="needs /proc")
def test_a_dropped_exported_stream_stops_its_workers(uneven: pathlib.Path) -> None:
    before = threads_now()
    stream = ppc.ParquetFile(uneven).stream(row_groups=[3, 0, 1, 2], batch_size=1, threads=4)
    reader = pa.RecordBatchReader.from_stream(stream)
    assert reader.read_next_batch().num_rows == 1
    del reader, stream
    gc.collect()
    assert threads_now() == before


@pytest.mark.parametrize(("limit", "started"), [(1, 0), (300, 0), (301, 2), (450, 2), (5000, 4)])
def test_a_limit_starts_only_the_row_groups_it_reaches(path: pathlib.Path, limit: int, started: int) -> None:
    f = ppc.ParquetFile(path)
    before = _lib._decode_threads_started()
    assert sum(b.num_rows for b in f.stream(limit=limit, threads=8)) == min(limit, 1000)
    assert _lib._decode_threads_started() - before == started


def test_a_batch_takes_no_row_group_past_its_last_row(path: pathlib.Path) -> None:
    f = ppc.ParquetFile(path)
    before = _lib._decode_threads_started()
    stream = iter(f.stream(batch_size=600, threads=2))
    assert pa.record_batch(next(stream)).num_rows == 600
    # row groups 0 and 1 make the batch; 2 is the one started when 0 was taken
    assert _lib._decode_threads_started() - before == 3
