//! Publishing rollups to Iceberg and opening SQL sessions over them.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use datafusion::arrow::array::{
    ArrayRef, Float64Builder, LargeBinaryBuilder, StringBuilder, TimestampMicrosecondBuilder,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::catalog::CatalogProvider;
use datafusion::datasource::provider_as_source;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::TableSource;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::{DataFileFormat, NestedField, PrimitiveType, Schema, Type};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_sql::{
    SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI, SQL_CATALOG_PROP_WAREHOUSE, SqlBindStyle,
    SqlCatalogBuilder,
};
use iceberg_datafusion::IcebergCatalogProvider;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use uuid::Uuid;

use crate::row::{Row, coarsen};
use crate::rule::{ChooseRollup, Rollup, RuleStats};

/// One table: its name and the width of its buckets in seconds.
#[derive(Clone, Copy, Debug)]
pub struct Resolution {
    pub table: &'static str,
    pub seconds: i64,
}

/// Finest first. The finest table is the one people query.
pub const RESOLUTIONS: [Resolution; 4] = [
    Resolution {
        table: "metrics",
        seconds: 10,
    },
    Resolution {
        table: "metrics_1m",
        seconds: 60,
    },
    Resolution {
        table: "metrics_10m",
        seconds: 600,
    },
    Resolution {
        table: "metrics_1h",
        seconds: 3600,
    },
];

const NAMESPACE: &str = "funnel";
const CATALOG_NAME: &str = "lake";
const WATERMARKS: &str = "funnel.watermarks";

/// Rows per Parquet row group. Small enough that a time filter can skip most of a file.
const ROW_GROUP_ROWS: usize = 32 * 1024;

pub struct Lake {
    catalog: Arc<dyn Catalog>,
}

fn schema() -> Result<Schema> {
    let string = || Type::Primitive(PrimitiveType::String);
    let double = || Type::Primitive(PrimitiveType::Double);
    let timestamp = || Type::Primitive(PrimitiveType::Timestamp);
    Ok(Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "tenant", string()).into(),
            NestedField::required(2, "metric", string()).into(),
            NestedField::required(3, "tags", string()).into(),
            NestedField::required(4, "kind", string()).into(),
            NestedField::required(5, "ts", timestamp()).into(),
            NestedField::required(6, "count", double()).into(),
            NestedField::required(7, "sum", double()).into(),
            NestedField::optional(8, "min", double()).into(),
            NestedField::optional(9, "max", double()).into(),
            NestedField::optional(10, "last", double()).into(),
            NestedField::optional(11, "last_at", timestamp()).into(),
            NestedField::optional(12, "sketch", Type::Primitive(PrimitiveType::Binary)).into(),
        ])
        .build()?)
}

impl Lake {
    /// Opens the lake in `dir`, creating the catalog and tables on first use.
    pub async fn open(dir: &Path) -> Result<Lake> {
        std::fs::create_dir_all(dir)?;
        let dir = dir.canonicalize()?;
        sqlx::any::install_default_drivers();
        let props = HashMap::from([
            (
                SQL_CATALOG_PROP_URI.to_string(),
                format!("sqlite:{}?mode=rwc", dir.join("catalog.db").display()),
            ),
            (
                SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                dir.join("warehouse").display().to_string(),
            ),
            (
                SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                SqlBindStyle::DollarNumeric.to_string(),
            ),
        ]);
        let catalog: Arc<dyn Catalog> = Arc::new(
            SqlCatalogBuilder::default()
                .with_storage_factory(Arc::new(LocalFsStorageFactory))
                .load(CATALOG_NAME, props)
                .await
                .context("opening the Iceberg catalog")?,
        );
        let namespace = NamespaceIdent::new(NAMESPACE.into());
        if !catalog.namespace_exists(&namespace).await? {
            catalog.create_namespace(&namespace, HashMap::new()).await?;
        }
        for r in RESOLUTIONS {
            let ident = TableIdent::new(namespace.clone(), r.table.into());
            if !catalog.table_exists(&ident).await? {
                let creation = TableCreation::builder()
                    .name(r.table.into())
                    .schema(schema()?)
                    .build();
                catalog.create_table(&namespace, creation).await?;
            }
        }
        Ok(Lake { catalog })
    }

    async fn table(&self, name: &str) -> Result<Table> {
        let ident = TableIdent::new(NamespaceIdent::new(NAMESPACE.into()), name.into());
        Ok(self.catalog.load_table(&ident).await?)
    }

    fn watermarks(table: &Table) -> BTreeMap<String, i64> {
        table
            .metadata()
            .current_snapshot()
            .and_then(|s| s.summary().additional_properties.get(WATERMARKS).cloned())
            .and_then(|json| serde_json::from_str(&json).ok())
            .unwrap_or_default()
    }

    /// The newest moment `tenant` is published up to, at every resolution.
    pub async fn watermark(&self, tenant: &str) -> Result<Option<i64>> {
        let mut oldest = None;
        for r in RESOLUTIONS {
            let table = self.table(r.table).await?;
            match Self::watermarks(&table).get(tenant) {
                Some(&w) => oldest = Some(oldest.map_or(w, |o: i64| o.min(w))),
                None => return Ok(None),
            }
        }
        Ok(oldest)
    }

    /// Publishes finest-resolution rows covering up to `until` (seconds) for `tenant`.
    ///
    /// Each table commits the rows together with its new watermark, in one Iceberg snapshot. A
    /// crash part way leaves some tables behind, and a retry skips the ones already done, so data
    /// lands exactly once per table.
    pub async fn publish(&self, tenant: &str, rows: &[Row], until: i64) -> Result<()> {
        for r in RESOLUTIONS {
            let table = self.table(r.table).await?;
            let mut marks = Self::watermarks(&table);
            if marks.get(tenant).is_some_and(|&w| w >= until) {
                continue;
            }
            let coarse = coarsen(rows, r.seconds).map_err(|e| anyhow::anyhow!("{e}"))?;
            let files = Self::write_rows(&table, &coarse).await?;
            marks.insert(tenant.to_owned(), until);
            let tx = Transaction::new(&table);
            let tx = tx
                .fast_append()
                .add_data_files(files)
                .set_snapshot_properties(HashMap::from([(
                    WATERMARKS.to_string(),
                    serde_json::to_string(&marks)?,
                )]))
                .apply(tx)?;
            tx.commit(self.catalog.as_ref()).await?;
        }
        Ok(())
    }

    async fn write_rows(table: &Table, rows: &[Row]) -> Result<Vec<iceberg::spec::DataFile>> {
        let iceberg_schema = table.metadata().current_schema().clone();
        let arrow_schema: SchemaRef = Arc::new(schema_to_arrow_schema(&iceberg_schema)?);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
            .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
            .build();
        let parquet = ParquetWriterBuilder::new(props, iceberg_schema);
        let rolling = RollingFileWriterBuilder::new_with_default_file_size(
            parquet,
            table.file_io().clone(),
            DefaultLocationGenerator::new(table.metadata())?,
            DefaultFileNameGenerator::new(
                "rollup".into(),
                Some(Uuid::new_v4().to_string()),
                DataFileFormat::Parquet,
            ),
        );
        let mut writer = DataFileWriterBuilder::new(rolling).build(None).await?;
        for chunk in rows.chunks(ROW_GROUP_ROWS) {
            writer.write(batch(&arrow_schema, chunk)?).await?;
        }
        Ok(writer.close().await?)
    }

    /// Opens a SQL session. With `rollups` off, every query reads the finest table, which is how
    /// tests prove the rule changes nothing but cost.
    pub async fn session(&self, rollups: bool) -> Result<(SessionContext, Arc<RuleStats>)> {
        let provider = IcebergCatalogProvider::try_new(self.catalog.clone()).await?;
        let schema = provider
            .schema(NAMESPACE)
            .context("funnel namespace is missing")?;

        let mut tables: Vec<Rollup> = Vec::new();
        for r in RESOLUTIONS {
            let table = self.table(r.table).await?;
            let marks = Self::watermarks(&table);
            let source: Arc<dyn TableSource> = provider_as_source(
                schema
                    .table(r.table)
                    .await?
                    .context("rollup table is missing")?,
            );
            tables.push(Rollup {
                seconds: r.seconds,
                covered_until: marks.values().copied().min(),
                source,
            });
        }

        let stats = Arc::new(RuleStats::default());
        let config = SessionConfig::new().with_default_catalog_and_schema(CATALOG_NAME, NAMESPACE);
        let mut builder = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features();
        if rollups {
            builder =
                builder.with_optimizer_rule(Arc::new(ChooseRollup::new(tables, stats.clone())));
        }
        let ctx = SessionContext::new_with_state(builder.build());
        ctx.register_catalog(CATALOG_NAME, Arc::new(provider));
        ctx.register_udaf(crate::quantile::udaf());
        Ok((ctx, stats))
    }
}

fn batch(schema: &SchemaRef, rows: &[Row]) -> Result<RecordBatch> {
    let mut tenant = StringBuilder::new();
    let mut metric = StringBuilder::new();
    let mut tags = StringBuilder::new();
    let mut kind = StringBuilder::new();
    let mut ts = TimestampMicrosecondBuilder::new();
    let mut count = Float64Builder::new();
    let mut sum = Float64Builder::new();
    let mut min = Float64Builder::new();
    let mut max = Float64Builder::new();
    let mut last = Float64Builder::new();
    let mut last_at = TimestampMicrosecondBuilder::new();
    let mut sketch = LargeBinaryBuilder::new();
    for r in rows {
        tenant.append_value(&r.tenant);
        metric.append_value(&r.metric);
        tags.append_value(&r.tags);
        kind.append_value(r.kind);
        ts.append_value(r.ts * 1_000_000);
        count.append_value(r.count);
        sum.append_value(r.sum);
        min.append_option(r.min);
        max.append_option(r.max);
        last.append_option(r.last);
        last_at.append_option(r.last_at.map(|t| t * 1_000_000));
        sketch.append_option(r.sketch.as_ref().map(|s| s.to_bytes()));
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(tenant.finish()),
        Arc::new(metric.finish()),
        Arc::new(tags.finish()),
        Arc::new(kind.finish()),
        Arc::new(ts.finish()),
        Arc::new(count.finish()),
        Arc::new(sum.finish()),
        Arc::new(min.finish()),
        Arc::new(max.finish()),
        Arc::new(last.finish()),
        Arc::new(last_at.finish()),
        Arc::new(sketch.finish()),
    ];
    Ok(RecordBatch::try_new(schema.clone(), columns)?)
}
