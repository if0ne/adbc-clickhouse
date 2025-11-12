mod get_info;
mod get_objects;

use std::borrow::Cow;

use clickhouse_arrow::{ClickHouseResponse, NativeClient, QueryParams, SettingValue};
use futures::StreamExt;

pub(crate) use get_info::*;
pub(crate) use get_objects::*;

pub enum Runtime {
    Handle(tokio::runtime::Handle),
    TokioRuntime(tokio::runtime::Runtime),
}

impl Runtime {
    pub fn new() -> std::io::Result<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            Ok(Self::Handle(handle))
        } else {
            let rt = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;

            Ok(Self::TokioRuntime(rt))
        }
    }

    pub fn block_on<F: Future>(&self, fut: F) -> F::Output {
        match self {
            Runtime::Handle(handle) => tokio::task::block_in_place(|| handle.block_on(fut)),
            Runtime::TokioRuntime(runtime) => runtime.block_on(fut),
        }
    }
}

#[derive(clickhouse_arrow::Row)]
pub(crate) struct SchemaRow {
    pub catalog_name: String,
    pub schema_name: String,
}

#[derive(clickhouse_arrow::Row)]
pub(crate) struct TableRow {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
}

#[derive(clickhouse_arrow::Row)]
pub(crate) struct ColumnRow {
    pub table_catalog: String,
    pub table_schema: String,
    pub table_name: String,
    pub table_type: String,
    pub column_name: String,
    pub ordianal_position: u64,
    pub remarks: String,
    pub xdbc_type_name: String,
    pub xdbc_column_size: Option<u64>,
    pub xdbc_decimal_digits: Option<u64>,
    pub xdbc_num_prec_radix: Option<u64>,
    pub xdbc_nullable: bool,
    pub xdbc_column_def: String,
    pub xdbc_datetime_sub: Option<u64>,
    pub xdbc_char_octet_length: Option<u64>,
    pub xdbc_is_nullable: String,
    pub xdbc_is_generatedcolumn: bool,
}

pub(crate) fn from_clickhouse_error(
    context: impl AsRef<str>,
    error: clickhouse_arrow::Error,
) -> adbc_core::error::Error {
    let context = context.as_ref();
    match &error {
        clickhouse_arrow::Error::Io(_) => adbc_core::error::Error::with_message_and_status(
            format!("[Clickhouse] {context}: {}", error),
            adbc_core::error::Status::IO,
        ),
        _ => adbc_core::error::Error::with_message_and_status(
            format!("[Clickhouse] {context}: {}", error),
            adbc_core::error::Status::Internal,
        ),
    }
}

const FETCH_ALL_BASE_SQL: &str = "SELECT
    c.table_catalog,
	c.table_schema,
	c.table_name,
	t.table_type,
	c.column_name,
	c.ordinal_position,
	c.column_comment as remarks,
	c.data_type as xdbc_type_name,
	c.character_maximum_length as xdbc_column_size,
	c.numeric_scale  as xdbc_decimal_digits,
	c.numeric_precision_radix  as xdbc_num_prec_radix,
	c.is_nullable::bool as xdbc_nullable,
	c.column_default as xdbc_column_def,
	c.datetime_precision as xdbc_datetime_sub,
	c.character_octet_length as xdbc_char_octet_length,
	CASE c.is_nullable
		WHEN 1 THEN 'YES'
		ELSE 'NO'
	END as xdbc_is_nullable,
	(countSubstrings(c.extra, 'GENERATED') > 0)::bool as xdbc_is_generatedcolumn
FROM
	INFORMATION_SCHEMA.COLUMNS c
JOIN INFORMATION_SCHEMA.`TABLES` t ON
	c.table_catalog = t.table_catalog AND c.table_schema = t.table_schema AND c.table_name = t.table_name";

const FETCH_MIN_TABLE_BASE_SQL: &str = "SELECT
	t.table_catalog,
	t.table_schema,
	t.table_name,
	t.table_type
FROM
	INFORMATION_SCHEMA.TABLES t";

const FETCH_MIN_SCHEMA_BASE_SQL: &str = "SELECT
	s.catalog_name,
	s.schema_name
FROM
	INFORMATION_SCHEMA.SCHEMATA s";

pub(crate) trait NativeClientExt {
    fn fetch_min_schemas(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
    ) -> impl Future<Output = Result<ClickHouseResponse<SchemaRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_min_schema_tables(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        table_type_filter: Option<Vec<String>>,
    ) -> impl Future<Output = Result<ClickHouseResponse<TableRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_all(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        table_type_filter: Option<Vec<String>>,
        column_filter: Option<String>,
    ) -> impl Future<Output = Result<ClickHouseResponse<ColumnRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_version(
        &self,
    ) -> impl Future<Output = Result<Option<String>, clickhouse_arrow::Error>> + Send;
}

impl NativeClientExt for NativeClient {
    async fn fetch_min_schemas(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
    ) -> Result<ClickHouseResponse<SchemaRow>, clickhouse_arrow::Error> {
        let mut pred: Vec<Cow<'static, str>> = vec![];
        let mut params = vec![];

        if let Some(catalog_filter) = catalog_filter {
            pred.push("s.catalog_name LIKE {catalog_filter:String}".into());
            params.push((
                "catalog_filter".to_string(),
                SettingValue::String(catalog_filter),
            ));
        }

        if let Some(schema_filter) = schema_filter {
            pred.push("s.schema_name LIKE {schema_filter:String}".into());
            params.push((
                "schema_filter".to_string(),
                SettingValue::String(schema_filter),
            ));
        }

        let (sql, params) = if !pred.is_empty() {
            let where_part: String = pred.join(" AND ");

            (
                format!(
                    "{FETCH_MIN_SCHEMA_BASE_SQL}
WHERE {where_part}"
                ),
                Some(QueryParams(params)),
            )
        } else {
            (FETCH_MIN_SCHEMA_BASE_SQL.to_string(), None)
        };

        self.query_params::<SchemaRow>(sql, params, None).await
    }

    async fn fetch_min_schema_tables(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        table_type_filter: Option<Vec<String>>,
    ) -> Result<ClickHouseResponse<TableRow>, clickhouse_arrow::Error> {
        let mut pred: Vec<Cow<'static, str>> = vec![];
        let mut params = vec![];

        if let Some(catalog_filter) = catalog_filter {
            pred.push("t.table_catalog LIKE {catalog_filter:String}".into());
            params.push((
                "catalog_filter".to_string(),
                SettingValue::String(catalog_filter),
            ));
        }

        if let Some(schema_filter) = schema_filter {
            pred.push("t.table_schema LIKE {schema_filter:String}".into());
            params.push((
                "schema_filter".to_string(),
                SettingValue::String(schema_filter),
            ));
        }

        if let Some(table_filter) = table_filter {
            pred.push("t.table_name LIKE {table_filter:String}".into());
            params.push((
                "table_filter".to_string(),
                SettingValue::String(table_filter),
            ));
        }

        if let Some(table_type_filter) = table_type_filter
            && !table_type_filter.is_empty()
        {
            let mut idents = vec![];
            table_type_filter
                .into_iter()
                .enumerate()
                .for_each(|(i, v)| {
                    idents.push(format!("{{table_type_filter_{i}:String}}"));
                    params.push((format!("table_type_filter_{i}"), SettingValue::String(v)));
                });

            let idents = idents.join(",");
            pred.push(format!("t.table_type IN ({idents})").into());
        }

        let (sql, params) = if !pred.is_empty() {
            let where_part: String = pred.join(" AND ");

            (
                format!(
                    "{FETCH_MIN_TABLE_BASE_SQL}
WHERE {where_part}"
                ),
                Some(QueryParams(params)),
            )
        } else {
            (FETCH_MIN_TABLE_BASE_SQL.to_string(), None)
        };

        self.query_params::<TableRow>(sql, params, None).await
    }

    async fn fetch_all(
        &self,
        catalog_filter: Option<String>,
        schema_filter: Option<String>,
        table_filter: Option<String>,
        table_type_filter: Option<Vec<String>>,
        column_filter: Option<String>,
    ) -> Result<ClickHouseResponse<ColumnRow>, clickhouse_arrow::Error> {
        let mut pred: Vec<Cow<'static, str>> = vec![];
        let mut params = vec![];

        if let Some(catalog_filter) = catalog_filter {
            pred.push("c.table_catalog LIKE {catalog_filter:String}".into());
            params.push((
                "catalog_filter".to_string(),
                SettingValue::String(catalog_filter),
            ));
        }

        if let Some(schema_filter) = schema_filter {
            pred.push("c.table_schema LIKE {schema_filter:String}".into());
            params.push((
                "schema_filter".to_string(),
                SettingValue::String(schema_filter),
            ));
        }

        if let Some(table_filter) = table_filter {
            pred.push("c.table_name LIKE {table_filter:String}".into());
            params.push((
                "table_filter".to_string(),
                SettingValue::String(table_filter),
            ));
        }

        if let Some(column_filter) = column_filter {
            pred.push("c.column_name LIKE {column_filter:String}".into());
            params.push((
                "column_filter".to_string(),
                SettingValue::String(column_filter),
            ));
        }

        if let Some(table_type_filter) = table_type_filter
            && !table_type_filter.is_empty()
        {
            let mut idents = vec![];
            table_type_filter
                .into_iter()
                .enumerate()
                .for_each(|(i, v)| {
                    idents.push(format!("{{table_type_filter_{i}:String}}"));
                    params.push((format!("table_type_filter_{i}"), SettingValue::String(v)));
                });

            let idents = idents.join(",");
            pred.push(format!("t.table_type IN ({idents})").into());
        }

        let (sql, params) = if !pred.is_empty() {
            let where_part: String = pred.join(" AND ");

            (
                format!(
                    "{FETCH_ALL_BASE_SQL}
WHERE {where_part}"
                ),
                Some(QueryParams(params)),
            )
        } else {
            (FETCH_ALL_BASE_SQL.to_string(), None)
        };

        self.query_params::<ColumnRow>(sql, params, None).await
    }

    async fn fetch_version(&self) -> Result<Option<String>, clickhouse_arrow::Error> {
        #[derive(clickhouse_arrow::Row)]
        struct ClickhouseVersion {
            version: String,
        }

        self.query_one::<ClickhouseVersion>("SELECT version() as version", None)
            .await
            .map(|v| v.map(|v| v.version))
    }
}

pub(crate) trait ClickhouseResponseExt<T> {
    fn collect_all(self) -> impl Future<Output = Result<Vec<T>, clickhouse_arrow::Error>> + Send;
}

impl<T: Send + 'static> ClickhouseResponseExt<T> for ClickHouseResponse<T> {
    async fn collect_all(self) -> Result<Vec<T>, clickhouse_arrow::Error> {
        self.collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
    }
}
