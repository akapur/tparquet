//! Writer options from Python arguments.

use crate::errors::{from_pcodec, invalid};
use pyo3::prelude::*;
use std::collections::BTreeMap;
use tparquet::parquet::basic::Compression;
use tparquet::parquet::file::properties::{EnabledStatistics, WriterProperties};
use tparquet::{ColumnCodec, Policy, WriterOptions};

/// The keyword arguments `write_table`, `Writer` and `transcode` share.
#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct Options {
    pub policy: String,
    pub columns: BTreeMap<String, String>,
    pub level: i64,
    pub base: String,
    pub row_group_size: Option<i64>,
    pub encodings: BTreeMap<String, String>,
    pub data_page_size: Option<i64>,
    pub data_page_row_count_limit: Option<i64>,
    pub write_batch_size: Option<i64>,
    pub dictionary: Option<bool>,
    /// Columns with a dictionary, which every other column then lacks.
    pub dictionary_columns: Vec<String>,
    pub statistics: Option<String>,
}

fn positive(name: &str, value: Option<i64>) -> PyResult<Option<usize>> {
    value
        .map(|n| {
            usize::try_from(n)
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| invalid(format!("{name} must be positive, got {n}")))
        })
        .transpose()
}

fn statistics(s: &str) -> PyResult<EnabledStatistics> {
    match s {
        "none" => Ok(EnabledStatistics::None),
        "chunk" => Ok(EnabledStatistics::Chunk),
        "page" => Ok(EnabledStatistics::Page),
        _ => Err(invalid(format!("unknown statistics level {s:?}: expected \"none\", \"chunk\" or \"page\""))),
    }
}

/// Parses a base codec such as `zstd(3)` or `snappy` (`tparquet::parse_base`).
pub fn base_codec(s: &str) -> PyResult<Compression> {
    tparquet::parse_base(s).map_err(from_pcodec)
}

impl Options {
    pub fn writer_options(&self) -> PyResult<WriterOptions> {
        let policy: Policy = self.policy.parse().map_err(from_pcodec)?;
        let mut o = WriterOptions::default().with_policy(policy).with_base(base_codec(&self.base)?);
        for (name, p) in &self.columns {
            o = o.with_column(name.clone(), p.parse().map_err(from_pcodec)?);
        }
        for name in &self.dictionary_columns {
            o = o.with_dictionary(name.clone(), true);
        }
        for (name, e) in &self.encodings {
            let encoding = e.parse().map_err(|e: tparquet::parquet::errors::ParquetError| invalid(e.to_string()))?;
            o = o.with_encoding(name.clone(), encoding);
        }
        let bad_level = || invalid(format!("level {} is not in 0..=12", self.level));
        let level = u32::try_from(self.level).map_err(|_| bad_level())?;
        o = o.with_level(level).map_err(|_| bad_level())?;
        let mut b = WriterProperties::builder();
        if let Some(n) = positive("row_group_size", self.row_group_size)? {
            b = b.set_max_row_group_row_count(Some(n));
        }
        if let Some(n) = positive("data_page_size", self.data_page_size)? {
            b = b.set_data_page_size_limit(n);
        }
        if let Some(n) = positive("data_page_row_count_limit", self.data_page_row_count_limit)? {
            b = b.set_data_page_row_count_limit(n);
        }
        if let Some(n) = positive("write_batch_size", self.write_batch_size)? {
            b = b.set_write_batch_size(n);
        }
        if let Some(d) = self.dictionary {
            b = b.set_dictionary_enabled(d);
        }
        if let Some(s) = &self.statistics {
            b = b.set_statistics_enabled(statistics(s)?);
        }
        o.properties = b.build();
        Ok(o)
    }
}

/// The name of a codec without its level, as `inspect` writes it (`PCODEC`, `ZSTD`...).
pub fn codec_name(c: Compression) -> String {
    let s = c.to_string();
    s.split_once('(').map_or(s.clone(), |(name, _)| name.to_string())
}

/// Leaf path to codec name.
pub fn codec_map(codecs: &[ColumnCodec]) -> BTreeMap<String, String> {
    codecs.iter().map(|c| (c.path.clone(), codec_name(c.codec))).collect()
}
