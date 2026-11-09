//! Long-term storage for Funnel: rollups in Apache Iceberg tables as Apache Parquet files, queried
//! with SQL through Apache DataFusion.
//!
//! Each hour of closed data is published at four resolutions, one table each: `metrics` (10
//! seconds), `metrics_1m`, `metrics_10m` and `metrics_1h`. People query `metrics`, and
//! [`rule::ChooseRollup`] moves the query to the coarsest table that gives the same answer.
//!
//! Iceberg makes the data readable outside Funnel. The catalog is the standard SQL catalog layout,
//! so Spark, Trino or anything else that speaks Iceberg can open the same tables.

pub mod lake;
pub mod quantile;
pub mod row;
pub mod rule;

pub use lake::{Lake, RESOLUTIONS, Resolution};
pub use row::Row;
