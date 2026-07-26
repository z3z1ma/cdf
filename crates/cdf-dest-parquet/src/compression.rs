use cdf_kernel::{CdfError, Result};
use parquet::basic::{Compression, ZstdLevel};

pub(crate) const PHYSICAL_PLAN_VERSION: u16 = 6;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ParquetCompression {
    None,
    Snappy,
    Lz4Raw,
    #[default]
    Zstd,
}

impl ParquetCompression {
    pub(crate) const ALL: [Self; 4] = [Self::None, Self::Snappy, Self::Lz4Raw, Self::Zstd];

    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Snappy => "snappy",
            Self::Lz4Raw => "lz4_raw",
            Self::Zstd => "zstd",
        }
    }

    pub(crate) const fn path_id(self) -> &'static str {
        match self {
            Self::None => "arrow_ipc_to_parquet_none",
            Self::Snappy => "arrow_ipc_to_parquet_snappy",
            Self::Lz4Raw => "arrow_ipc_to_parquet_lz4_raw",
            Self::Zstd => "arrow_ipc_to_parquet_zstd",
        }
    }

    pub(crate) fn from_name(value: &str) -> Result<Self> {
        match value {
            "none" | "uncompressed" => Ok(Self::None),
            "snappy" => Ok(Self::Snappy),
            "lz4" | "lz4_raw" => Ok(Self::Lz4Raw),
            "zstd" => Ok(Self::Zstd),
            _ => Err(CdfError::contract(format!(
                "Parquet destination compression `{value}` is unsupported; expected none, snappy, lz4, or zstd"
            ))),
        }
    }

    pub(crate) fn from_path_id(value: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|compression| compression.path_id() == value)
            .ok_or_else(|| {
                CdfError::contract(format!(
                    "Parquet staged ingress received unknown physical path `{value}`"
                ))
            })
    }

    pub(crate) fn codec(self) -> Result<Compression> {
        match self {
            Self::None => Ok(Compression::UNCOMPRESSED),
            Self::Snappy => Ok(Compression::SNAPPY),
            Self::Lz4Raw => Ok(Compression::LZ4_RAW),
            Self::Zstd => ZstdLevel::try_new(1)
                .map(Compression::ZSTD)
                .map_err(|error| {
                    CdfError::contract(format!("invalid Parquet zstd level: {error}"))
                }),
        }
    }
}
