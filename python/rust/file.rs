//! Reading: an open Parquet file (PCODEC or not), its metadata, and streams of its rows.

use crate::errors::{detached, from_arrow, from_parquet, from_pcodec, invalid, os_error};
use crate::ffi::{ArrowSchema, RecordBatchStream, named};
use crate::options::codec_name;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{Array, ArrayRef, DictionaryArray, RecordBatchOptions};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_schema::{ArrowError, DataType, SchemaRef, TimeUnit};
use arrow_select::concat::concat;
use bytes::Bytes;
use pyo3::prelude::*;
use pyo3::pybacked::PyBackedBytes;
use pyo3::types::PyDict;
use std::collections::{BTreeMap, VecDeque};
use std::io::Read;
use std::ops::Range;
use std::path::PathBuf;
use std::sync::Arc;
use tparquet::parquet::DecodeResult;
use tparquet::parquet::arrow::ProjectionMask;
use tparquet::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use tparquet::parquet::arrow::push_decoder::{ParquetPushDecoder, ParquetPushDecoderBuilder};
use tparquet::parquet::errors::{ParquetError, Result as ParquetResult};
use tparquet::parquet::file::reader::{ChunkReader, Length};
use tparquet::parquet::file::statistics::Statistics;

/// Where the file's bytes are.
#[derive(Clone)]
enum Source {
    /// The file opened when the footer was read, kept open so that every read sees that
    /// file even when its path is replaced later; read at given offsets, so that readers in
    /// other threads do not share a cursor.
    File {
        path: PathBuf,
        file: Arc<std::fs::File>,
        len: u64,
    },
    Bytes(Bytes),
}

/// One handle on a [`Source`].
pub enum Reader {
    File { file: Arc<std::fs::File>, len: u64 },
    Bytes(Bytes),
}

/// Reads `buf.len()` bytes of `file` (or fewer at its end) at `offset`, without its cursor.
fn read_at(file: &std::fs::File, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
    #[cfg(unix)]
    return std::os::unix::fs::FileExt::read_at(file, buf, offset);
    #[cfg(windows)]
    return std::os::windows::fs::FileExt::seek_read(file, buf, offset);
}

/// A reader of a file from an offset, with a position of its own.
struct ReadAt {
    file: Arc<std::fs::File>,
    pos: u64,
}

impl Read for ReadAt {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = read_at(&self.file, buf, self.pos)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl Length for Reader {
    fn len(&self) -> u64 {
        match self {
            Self::File { len, .. } => *len,
            Self::Bytes(b) => Length::len(b),
        }
    }
}

impl ChunkReader for Reader {
    type T = Box<dyn Read + Send>;

    fn get_read(&self, start: u64) -> ParquetResult<Self::T> {
        Ok(match self {
            Self::File { file, .. } => Box::new(ReadAt { file: file.clone(), pos: start }),
            Self::Bytes(b) => Box::new(b.get_read(start)?),
        })
    }

    fn get_bytes(&self, start: u64, length: usize) -> ParquetResult<Bytes> {
        match self {
            Self::File { file, len } => {
                // checked before allocating: a length from a corrupt footer allocates no more
                // than the file holds
                if start.checked_add(length as u64).is_none_or(|end| end > *len) {
                    return Err(ParquetError::EOF(format!("{length} bytes at {start} are past the end of the file")));
                }
                let mut buf = vec![0; length];
                read_exact_at(file, &mut buf, start)?;
                Ok(buf.into())
            }
            Self::Bytes(b) => b.get_bytes(start, length),
        }
    }
}

fn read_exact_at(file: &std::fs::File, mut buf: &mut [u8], mut offset: u64) -> std::io::Result<()> {
    while !buf.is_empty() {
        match read_at(file, buf, offset) {
            Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
            Ok(n) => {
                buf = &mut buf[n..];
                offset += n as u64;
            }
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

/// Ranges this close together are read as one: reading the gap costs less than another seek on
/// a disk, and than another read of a record that a file system does not cache.
const MAX_GAP: u64 = 1 << 20;

impl Reader {
    /// The bytes of each of `ranges`, with one read per run of ranges at most [`MAX_GAP`] apart.
    fn fetch(&self, ranges: &[Range<u64>]) -> ParquetResult<Vec<Bytes>> {
        if let Some(r) = ranges.iter().find(|r| r.end < r.start) {
            return Err(ParquetError::General(format!("byte range {r:?} ends before it starts")));
        }
        let mut order: Vec<usize> = (0..ranges.len()).collect();
        order.sort_unstable_by_key(|&i| ranges[i].start);
        let mut out = vec![Bytes::new(); ranges.len()];
        let mut run = order.as_slice();
        while let Some(&first) = run.first() {
            let start = ranges[first].start;
            let mut end = ranges[first].end;
            let n = run
                .iter()
                .take_while(|&&i| {
                    let joins = ranges[i].start <= end.saturating_add(MAX_GAP);
                    if joins {
                        end = end.max(ranges[i].end);
                    }
                    joins
                })
                .count();
            let length =
                usize::try_from(end - start).map_err(|_| ParquetError::EOF(format!("range {start}..{end}")))?;
            let data = self.get_bytes(start, length)?;
            for &i in &run[..n] {
                out[i] = data.slice((ranges[i].start - start) as usize..(ranges[i].end - start) as usize);
            }
            run = &run[n..];
        }
        Ok(out)
    }
}

impl Source {
    fn open_path(path: PathBuf) -> PyResult<Self> {
        let file = std::fs::File::open(&path).map_err(|e| os_error(&e, &path))?;
        let len = file.metadata().map_err(|e| os_error(&e, &path))?.len();
        Ok(Self::File { path, file: Arc::new(file), len })
    }

    fn reader(&self) -> Reader {
        match self {
            Self::File { file, len, .. } => Reader::File { file: file.clone(), len: *len },
            Self::Bytes(b) => Reader::Bytes(b.clone()),
        }
    }

    /// The name read errors give the file: its path, if it has one.
    fn name(&self) -> Option<String> {
        match self {
            Self::File { path, .. } => Some(path.display().to_string()),
            Self::Bytes(_) => None,
        }
    }
}

/// A Parquet file whose footer has been read.
#[pyclass(frozen, module = "tparquet._lib")]
pub struct File {
    source: Source,
    metadata: ArrowReaderMetadata,
}

impl File {
    fn load(py: Python<'_>, source: impl FnOnce() -> PyResult<Source> + Send) -> PyResult<Self> {
        detached(py, || {
            let source = source()?;
            let reader = source.reader();
            let metadata = ArrowReaderMetadata::load(&reader, ArrowReaderOptions::new()).map_err(from_parquet)?;
            Ok(Self { source, metadata })
        })
    }

    pub fn reader(&self) -> Reader {
        self.source.reader()
    }

    /// The Arrow schema of the file.
    pub fn schema_ref(&self) -> SchemaRef {
        self.metadata.schema().clone()
    }

    /// For the top-level `columns` (all by default): their root indices in file order, where
    /// each requested column is among those, and the schema of the requested columns in the
    /// requested order. The reader's own schema lacks the file's schema metadata: this one is
    /// the file's, projected.
    fn projection(&self, columns: Option<Vec<String>>) -> PyResult<(Vec<usize>, Vec<usize>, SchemaRef)> {
        let file_schema = self.metadata.schema();
        let wanted = match columns {
            Some(columns) => roots(file_schema, &columns)?,
            None => (0..file_schema.fields().len()).collect(),
        };
        let mut sorted = wanted.clone();
        sorted.sort_unstable();
        let order: Vec<usize> = wanted.iter().map(|i| sorted.binary_search(i).unwrap_or_default()).collect();
        let schema = Arc::new(file_schema.project(&sorted).and_then(|s| s.project(&order)).map_err(from_arrow)?);
        Ok((sorted, order, schema))
    }
}

/// Opens the Parquet file at `path`.
#[pyfunction]
pub fn open_path(py: Python<'_>, path: PathBuf) -> PyResult<File> {
    File::load(py, || Source::open_path(path))
}

/// Opens a Parquet file held in `data`.
#[pyfunction]
pub fn open_bytes(py: Python<'_>, data: PyBackedBytes) -> PyResult<File> {
    File::load(py, || Ok(Source::Bytes(Bytes::from_owner(data))))
}

/// The batches of a push decoder, fed by `reader` with the byte ranges it asks for: whole
/// column chunks, rather than each page as the page reader reaches it. The decoder ends a batch
/// at each row group; these batches run across row groups, `batch_size` rows each but the last.
struct Decoded {
    decoder: ParquetPushDecoder,
    reader: Reader,
    batch_size: usize,
    pending: VecDeque<RecordBatch>,
    pending_rows: usize,
    failed: bool,
}

impl Decoded {
    fn new(decoder: ParquetPushDecoder, reader: Reader, batch_size: usize) -> Self {
        Self { decoder, reader, batch_size, pending: VecDeque::new(), pending_rows: 0, failed: false }
    }

    fn next_batch(&mut self) -> Result<Option<RecordBatch>, ArrowError> {
        while self.pending_rows < self.batch_size {
            match self.next_decoded()? {
                Some(batch) if self.pending.is_empty() && batch.num_rows() == self.batch_size => {
                    return Ok(Some(batch));
                }
                Some(batch) => {
                    self.pending_rows += batch.num_rows();
                    self.pending.push_back(batch);
                }
                None if self.pending.is_empty() => return Ok(None),
                None => break,
            }
        }
        self.take_pending(self.batch_size.min(self.pending_rows)).map(Some)
    }

    fn take_pending(&mut self, rows: usize) -> Result<RecordBatch, ArrowError> {
        let mut parts = Vec::new();
        let mut needed = rows;
        while needed > 0 {
            let Some(front) = self.pending.pop_front() else { break };
            if front.num_rows() > needed {
                parts.push(front.slice(0, needed));
                self.pending.push_front(front.slice(needed, front.num_rows() - needed));
                needed = 0;
            } else {
                needed -= front.num_rows();
                parts.push(front);
            }
        }
        self.pending_rows -= rows;
        match parts.as_slice() {
            [one] => Ok(one.clone()),
            _ => concat_parts(&parts, rows),
        }
    }

    fn next_decoded(&mut self) -> ParquetResult<Option<RecordBatch>> {
        loop {
            match self.decoder.try_decode()? {
                DecodeResult::Data(batch) => return Ok(Some(batch)),
                DecodeResult::Finished => return Ok(None),
                DecodeResult::NeedsData(ranges) => {
                    let data = self.reader.fetch(&ranges)?;
                    self.decoder.push_ranges(ranges, data)?;
                }
            }
        }
    }
}

impl Iterator for Decoded {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let batch = self.next_batch().transpose()?;
        self.failed = batch.is_err();
        Some(batch)
    }
}

/// `parts` as one batch. Row groups have dictionaries of their own, usually equal: a dictionary
/// column keeps one of them when they are, as the page reader's batches do.
fn concat_parts(parts: &[RecordBatch], rows: usize) -> Result<RecordBatch, ArrowError> {
    let schema = parts[0].schema();
    let columns = (0..schema.fields().len())
        .map(|c| {
            let arrays: Vec<&dyn Array> = parts.iter().map(|p| p.column(c).as_ref()).collect();
            let shared = match arrays[0].data_type() {
                DataType::Dictionary(key, _) => match key.as_ref() {
                    DataType::Int8 => concat_keys::<Int8Type>(&arrays)?,
                    DataType::Int16 => concat_keys::<Int16Type>(&arrays)?,
                    DataType::Int32 => concat_keys::<Int32Type>(&arrays)?,
                    DataType::Int64 => concat_keys::<Int64Type>(&arrays)?,
                    DataType::UInt8 => concat_keys::<UInt8Type>(&arrays)?,
                    DataType::UInt16 => concat_keys::<UInt16Type>(&arrays)?,
                    DataType::UInt32 => concat_keys::<UInt32Type>(&arrays)?,
                    DataType::UInt64 => concat_keys::<UInt64Type>(&arrays)?,
                    _ => None,
                },
                _ => None,
            };
            shared.map_or_else(|| concat(&arrays), Ok)
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new_with_options(schema, columns, &RecordBatchOptions::new().with_row_count(Some(rows)))
}

/// The dictionary arrays `arrays` as one, if they share their dictionary's values.
fn concat_keys<K: ArrowDictionaryKeyType>(arrays: &[&dyn Array]) -> Result<Option<ArrayRef>, ArrowError> {
    let dictionaries: Vec<&DictionaryArray<K>> = arrays.iter().map(|a| a.as_dictionary::<K>()).collect();
    let values = dictionaries[0].values();
    if dictionaries[1..].iter().any(|d| d.values().to_data() != values.to_data()) {
        return Ok(None);
    }
    let keys: Vec<&dyn Array> = dictionaries.iter().map(|d| d.keys() as &dyn Array).collect();
    let keys = concat(&keys)?.as_primitive::<K>().clone();
    Ok(Some(Arc::new(DictionaryArray::<K>::try_new(keys, values.clone())?)))
}

/// Batches of a projected reader with the columns in the requested order; its errors name
/// the file when it has a name.
struct Reordered {
    inner: Decoded,
    order: Vec<usize>,
    schema: SchemaRef,
    name: Option<String>,
}

impl Iterator for Reordered {
    type Item = Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let batch = self.inner.next()?.and_then(|b| b.project(&self.order));
        Some(batch.map_err(|e| match &self.name {
            Some(name) => named(name, e),
            None => e,
        }))
    }
}

impl RecordBatchReader for Reordered {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// The root indices of `columns` in the file, in request order.
fn roots(schema: &arrow_schema::Schema, columns: &[String]) -> PyResult<Vec<usize>> {
    let mut indices = Vec::with_capacity(columns.len());
    for name in columns {
        let i = schema.index_of(name).map_err(|_| invalid(format!("no column named '{name}'")))?;
        if indices.contains(&i) {
            return Err(invalid(format!("column '{name}' is requested more than once")));
        }
        indices.push(i);
    }
    Ok(indices)
}

#[pymethods]
impl File {
    /// The Arrow schema of the file.
    fn schema(&self) -> ArrowSchema {
        ArrowSchema(self.metadata.schema().clone())
    }

    /// A stream of the file's rows: the given top-level `columns` in that order, the given
    /// `row_groups` in that order, at most `limit` rows.
    #[pyo3(signature = (columns=None, row_groups=None, batch_size=65536, limit=None))]
    fn stream(
        &self,
        columns: Option<Vec<String>>,
        row_groups: Option<Vec<i64>>,
        batch_size: i64,
        limit: Option<i64>,
    ) -> PyResult<RecordBatchStream> {
        let batch_size = usize::try_from(batch_size)
            .ok()
            .filter(|n| *n > 0)
            .ok_or_else(|| invalid(format!("batch_size must be positive, got {batch_size}")))?;
        let mut builder =
            ParquetPushDecoderBuilder::new_with_metadata(self.metadata.clone()).with_batch_size(batch_size);
        if let Some(rgs) = row_groups {
            let n = self.metadata.metadata().num_row_groups();
            let rgs = rgs
                .iter()
                .map(|&rg| {
                    usize::try_from(rg)
                        .ok()
                        .filter(|i| *i < n)
                        .ok_or_else(|| invalid(format!("row group {rg} out of range (the file has {n})")))
                })
                .collect::<PyResult<Vec<_>>>()?;
            builder = builder.with_row_groups(rgs);
        }
        if let Some(limit) = limit {
            let limit =
                usize::try_from(limit).map_err(|_| invalid(format!("limit must not be negative, got {limit}")))?;
            builder = builder.with_limit(limit);
        }
        let (sorted, order, schema) = self.projection(columns)?;
        let mask = ProjectionMask::roots(self.metadata.parquet_schema(), sorted);
        let decoder = builder.with_projection(mask).build().map_err(from_parquet)?;
        let inner = Decoded::new(decoder, self.reader(), batch_size);
        let name = self.source.name();
        Ok(RecordBatchStream::new(Box::new(Reordered { inner, order, schema, name })))
    }

    /// A stream without rows, with the schema `stream(columns)` has.
    #[pyo3(signature = (columns=None))]
    fn empty(&self, columns: Option<Vec<String>>) -> PyResult<RecordBatchStream> {
        let (_, _, schema) = self.projection(columns)?;
        Ok(RecordBatchStream::new(Box::new(RecordBatchIterator::new(vec![], schema))))
    }

    /// The footer as a dict: rows, row groups, per leaf column its physical type and codecs.
    fn metadata<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let md = self.metadata.metadata();
        let file = md.file_metadata();
        let d = PyDict::new(py);
        d.set_item("num_rows", file.num_rows())?;
        d.set_item("row_group_rows", md.row_groups().iter().map(|rg| rg.num_rows()).collect::<Vec<_>>())?;
        d.set_item("created_by", file.created_by())?;
        let kv: Vec<(String, Option<String>)> = file
            .key_value_metadata()
            .map(|kv| kv.iter().map(|kv| (kv.key.clone(), kv.value.clone())).collect())
            .unwrap_or_default();
        d.set_item("key_value_metadata", kv)?;
        let columns = file
            .schema_descr()
            .columns()
            .iter()
            .enumerate()
            .map(|(i, c)| {
                let mut codecs: Vec<String> = vec![];
                for rg in md.row_groups() {
                    let name = codec_name(rg.column(i).compression());
                    if !codecs.contains(&name) {
                        codecs.push(name);
                    }
                }
                (c.path().string(), c.physical_type().to_string(), codecs)
            })
            .collect::<Vec<_>>();
        d.set_item("columns", columns)?;
        Ok(d)
    }

    /// Per row group, the bounds its statistics give for each `INT32` and `INT64` leaf:
    /// `{path: (min, max)}`, the values as stored (signed). Leaves without both bounds are left
    /// out, and so are dotted paths that name two leaves (a top-level field `a.b` and the field
    /// `b` of a struct `a`).
    fn integer_bounds(&self) -> Vec<BTreeMap<String, (i64, i64)>> {
        let md = self.metadata.metadata();
        let paths: Vec<String> =
            md.file_metadata().schema_descr().columns().iter().map(|c| c.path().string()).collect();
        let unique = |path: &String| paths.iter().filter(|p| *p == path).count() == 1;
        md.row_groups()
            .iter()
            .map(|rg| {
                rg.columns()
                    .iter()
                    .filter_map(|c| Some((c.column_path().string(), bounds(c.statistics()?)?)))
                    .filter(|(path, _)| unique(path))
                    .collect()
            })
            .collect()
    }

    /// The statistics of the top-level columns that hold signed integers, dates or
    /// timestamps: how each is stored (`int`, `date32`, `date64` or `timestamp[<unit>]`,
    /// dictionaries by their values), and per row group `{name: (min, max)}` of those whose
    /// statistics give both bounds. A name is a top-level field, never a nested leaf with the
    /// same dotted path.
    #[allow(clippy::type_complexity)]
    fn column_bounds(&self) -> (BTreeMap<String, String>, Vec<BTreeMap<String, (i64, i64)>>) {
        let md = self.metadata.metadata();
        let leaves = md.file_metadata().schema_descr().columns();
        let mut kinds = BTreeMap::new();
        let mut columns = vec![];
        for field in self.metadata.schema().fields() {
            let kind = match field.data_type() {
                DataType::Dictionary(_, values) => stored_kind(values),
                t => stored_kind(t),
            };
            let leaf = leaves.iter().position(|c| c.path().parts() == std::slice::from_ref(field.name()));
            if let (Some(kind), Some(leaf)) = (kind, leaf) {
                kinds.insert(field.name().clone(), kind);
                columns.push((field.name().clone(), leaf));
            }
        }
        let bounds = md
            .row_groups()
            .iter()
            .map(|rg| {
                columns
                    .iter()
                    .filter_map(|(name, leaf)| Some((name.clone(), bounds(rg.column(*leaf).statistics()?)?)))
                    .collect()
            })
            .collect();
        (kinds, bounds)
    }

    /// The `tparquet inspect --json` report.
    fn inspect(&self, py: Python<'_>) -> PyResult<String> {
        detached(py, || {
            let report = tparquet::inspect::inspect(&self.reader()).map_err(from_pcodec)?;
            Ok(report.to_json().to_string())
        })
    }
}

/// The bounds of `INT32` and `INT64` statistics, as stored (signed).
fn bounds(s: &Statistics) -> Option<(i64, i64)> {
    match s {
        Statistics::Int32(s) => Some((i64::from(*s.min_opt()?), i64::from(*s.max_opt()?))),
        Statistics::Int64(s) => Some((*s.min_opt()?, *s.max_opt()?)),
        _ => None,
    }
}

/// How a column of type `t` is stored, for the types whose statistics bound signed integers.
fn stored_kind(t: &DataType) -> Option<String> {
    Some(match t {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => "int".into(),
        DataType::Date32 => "date32".into(),
        DataType::Date64 => "date64".into(),
        DataType::Timestamp(unit, _) => format!(
            "timestamp[{}]",
            match unit {
                TimeUnit::Second => "s",
                TimeUnit::Millisecond => "ms",
                TimeUnit::Microsecond => "us",
                TimeUnit::Nanosecond => "ns",
            }
        ),
        _ => return None,
    })
}

/// Where `a` and `b` first differ (floats compared bit for bit), or `None`.
#[pyfunction]
pub fn verify(py: Python<'_>, a: &File, b: &File) -> PyResult<Option<String>> {
    detached(py, || {
        let d = tparquet::verify::verify(a.reader(), b.reader()).map_err(from_pcodec)?;
        Ok(d.map(|d| d.to_string()))
    })
}
