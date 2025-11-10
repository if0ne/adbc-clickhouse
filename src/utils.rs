mod get_info;
mod get_objects;

use adbc_core::options::ObjectDepth;
use clickhouse_arrow::{ClickHouseResponse, NativeClient, QueryParams, SettingValue};
use futures::StreamExt;

pub(crate) use get_info::*;
pub(crate) use get_objects::*;

#[derive(clickhouse_arrow::Row)]
pub(crate) struct SchemaRow {
    pub name: String,
}

#[derive(clickhouse_arrow::Row)]
pub(crate) struct TableRow {
    pub name: String,
}

#[derive(clickhouse_arrow::Row)]
pub(crate) struct ColumnRow {
    pub name: String,
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

pub(crate) fn is_schema_required(depth: &ObjectDepth) -> bool {
    matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Schemas | ObjectDepth::Tables | ObjectDepth::Columns
    )
}

pub(crate) fn is_table_required(depth: &ObjectDepth) -> bool {
    matches!(
        depth,
        ObjectDepth::All | ObjectDepth::Tables | ObjectDepth::Columns
    )
}

pub(crate) fn is_column_required(depth: &ObjectDepth) -> bool {
    matches!(depth, ObjectDepth::All | ObjectDepth::Columns)
}

pub(crate) trait NativeClientExt {
    fn fetch_schemas(
        &self,
        filter: impl Into<String> + Send,
    ) -> impl Future<Output = Result<ClickHouseResponse<SchemaRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_schema_tables(
        &self,
        schema: impl Into<String> + Send,
        filter: impl Into<String> + Send,
    ) -> impl Future<Output = Result<ClickHouseResponse<TableRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_table_columns(
        &self,
        schema: impl Into<String> + Send,
        table: impl Into<String> + Send,
        filter: impl Into<String> + Send,
    ) -> impl Future<Output = Result<ClickHouseResponse<ColumnRow>, clickhouse_arrow::Error>> + Send;

    fn fetch_version(
        &self,
    ) -> impl Future<Output = Result<Option<String>, clickhouse_arrow::Error>> + Send;
}

impl NativeClientExt for NativeClient {
    async fn fetch_schemas(
        &self,
        filter: impl Into<String> + Send,
    ) -> Result<ClickHouseResponse<SchemaRow>, clickhouse_arrow::Error> {
        self.query_params::<SchemaRow>(
            "SELECT name FROM system.databases where name LIKE {db:String} AND name <> 'INFORMATION_SCHEMA' AND name <> 'information_schema' AND name <> 'system'",
            Some(QueryParams(vec![
                ("db".to_string(), SettingValue::String(filter.into())),
            ])),
            None
        ).await
    }

    async fn fetch_schema_tables(
        &self,
        schema: impl Into<String> + Send,
        filter: impl Into<String> + Send,
    ) -> Result<ClickHouseResponse<TableRow>, clickhouse_arrow::Error> {
        self.query_params::<TableRow>(
            "SELECT name FROM system.tables WHERE database = {db:String} and table LIKE {table:String}",
            Some(QueryParams(vec![
                ("db".to_string(), SettingValue::String(schema.into())),
                ("table".to_string(), SettingValue::String(filter.into())),
            ])),
            None
        ).await
    }

    async fn fetch_table_columns(
        &self,
        schema: impl Into<String> + Send,
        table: impl Into<String> + Send,
        filter: impl Into<String> + Send,
    ) -> Result<ClickHouseResponse<ColumnRow>, clickhouse_arrow::Error> {
        self.query_params::<ColumnRow>(
            "SELECT name FROM system.columns WHERE database = {db:String} AND table = {table:String} AND name LIKE {column:String}",
            Some(QueryParams(vec![
                ("db".to_string(), SettingValue::String(schema.into())),
                ("table".to_string(), SettingValue::String(table.into())),
                ("column".to_string(), SettingValue::String(filter.into()))
            ])),
            None
        ).await
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
