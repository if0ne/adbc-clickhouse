use adbc_core::{error::Result, options::ObjectDepth, schemas};
use arrow_array::*;
use clickhouse_arrow::NativeClient;
use itertools::Itertools;

use crate::{Catalog, ColumnSchema, DbSchema, TableSchema, utils::from_clickhouse_error};

use super::{ClickhouseResponseExt, NativeClientExt};

pub(crate) struct GetObjectsBuilder<'a> {
    catalog_filter: Option<&'a str>,
    schema_filter: Option<&'a str>,
    table_filter: Option<&'a str>,
    table_type_filter: Option<Vec<&'a str>>,
    column_filter: Option<&'a str>,
}

impl<'a> GetObjectsBuilder<'a> {
    pub fn new(
        catalog_filter: Option<&'a str>,
        schema_filter: Option<&'a str>,
        table_filter: Option<&'a str>,
        table_type_filter: Option<Vec<&'a str>>,
        column_filter: Option<&'a str>,
    ) -> GetObjectsBuilder<'a> {
        GetObjectsBuilder {
            catalog_filter,
            schema_filter,
            table_filter,
            table_type_filter,
            column_filter,
        }
    }

    pub async fn build(
        self,
        native_client: &NativeClient,
        depth: &ObjectDepth,
    ) -> Result<RecordBatch> {
        let catalogs = match depth {
            ObjectDepth::All | ObjectDepth::Columns => self.fetch_all(native_client).await,
            ObjectDepth::Catalogs => self.fetch_min_catalogs(native_client).await,
            ObjectDepth::Schemas => self.fetch_min_schemas(native_client).await,
            ObjectDepth::Tables => self.fetch_min_tables(native_client).await,
        }?;

        let record_batch =
            serde_arrow::to_record_batch(schemas::GET_OBJECTS_SCHEMA.fields(), &catalogs).map_err(
                |err| {
                    adbc_core::error::Error::with_message_and_status(
                        format!("Failed to serialize catalogs: {err}"),
                        adbc_core::error::Status::Internal,
                    )
                },
            )?;

        Ok(record_batch)
    }

    async fn fetch_all(&self, native_client: &NativeClient) -> Result<Vec<Catalog>> {
        let columns = native_client
            .fetch_all(
                self.catalog_filter.map(|v| v.to_string()),
                self.schema_filter.map(|v| v.to_string()),
                self.table_filter.map(|v| v.to_string()),
                self.table_type_filter
                    .as_ref()
                    .map(|v| v.iter().map(|v| v.to_string()).collect()),
                self.column_filter.map(|v| v.to_string()),
            )
            .await
            .map_err(|err| from_clickhouse_error("Failed to fetch tables", err))?
            .collect_all()
            .await
            .map_err(|err| from_clickhouse_error("Failed to parse tables", err))?;

        let catalogs = columns
            .into_iter()
            .into_group_map_by(|v| v.table_catalog.clone())
            .into_iter()
            .map(|(catalog_name, schemas)| {
                let schemas = schemas
                    .into_iter()
                    .into_group_map_by(|v| v.table_schema.clone());
                let schemas = schemas
                    .into_iter()
                    .map(|(k, v)| {
                        let tables = v
                            .into_iter()
                            .into_group_map_by(|v| (v.table_name.clone(), v.table_type.clone()));

                        DbSchema {
                            db_schema_name: Some(k),
                            db_schema_tables: Some(
                                tables
                                    .into_iter()
                                    .map(|((name, ty), v)| TableSchema {
                                        table_name: name,
                                        table_type: ty,
                                        table_columns: Some(
                                            v.into_iter()
                                                .map(|v| ColumnSchema {
                                                    column_name: v.column_name,
                                                    ordinal_position: Some(
                                                        v.ordianal_position as i32,
                                                    ),
                                                    remarks: Some(v.remarks),
                                                    xdbc_data_type: None,
                                                    xdbc_type_name: Some(v.xdbc_type_name),
                                                    xdbc_column_size: v
                                                        .xdbc_column_size
                                                        .map(|v| v as i32),
                                                    xdbc_decimal_digits: v
                                                        .xdbc_decimal_digits
                                                        .map(|v| v as i16),
                                                    xdbc_num_prec_radix: v
                                                        .xdbc_num_prec_radix
                                                        .map(|v| v as i16),
                                                    xdbc_nullable: Some(if v.xdbc_nullable {
                                                        0
                                                    } else {
                                                        1
                                                    }),
                                                    xdbc_column_def: Some(v.xdbc_column_def),
                                                    xdbc_sql_data_type: None,
                                                    xdbc_datetime_sub: v
                                                        .xdbc_datetime_sub
                                                        .map(|v| v as i16),
                                                    xdbc_char_octet_length: v
                                                        .xdbc_char_octet_length
                                                        .map(|v| v as i32),
                                                    xdbc_is_nullable: Some(v.xdbc_is_nullable),
                                                    xdbc_scope_catalog: None,
                                                    xdbc_scope_schema: None,
                                                    xdbc_scope_table: None,
                                                    xdbc_is_autoincrement: None,
                                                    xdbc_is_generatedcolumn: Some(
                                                        v.xdbc_is_generatedcolumn,
                                                    ),
                                                })
                                                .collect(),
                                        ),
                                        table_constraints: None,
                                    })
                                    .collect(),
                            ),
                        }
                    })
                    .collect();

                Catalog {
                    catalog_name: Some(catalog_name),
                    catalog_db_schemas: Some(schemas),
                }
            })
            .collect();

        Ok(catalogs)
    }

    async fn fetch_min_tables(&self, native_client: &NativeClient) -> Result<Vec<Catalog>> {
        let tables = native_client
            .fetch_min_schema_tables(
                self.catalog_filter.map(|v| v.to_string()),
                self.schema_filter.map(|v| v.to_string()),
                self.table_filter.map(|v| v.to_string()),
                self.table_type_filter
                    .as_ref()
                    .map(|v| v.iter().map(|v| v.to_string()).collect()),
            )
            .await
            .map_err(|err| from_clickhouse_error("Failed to fetch tables", err))?
            .collect_all()
            .await
            .map_err(|err| from_clickhouse_error("Failed to parse tables", err))?;

        let catalogs = tables
            .into_iter()
            .into_group_map_by(|v| v.table_catalog.clone())
            .into_iter()
            .map(|(catalog_name, schemas)| {
                let schemas = schemas
                    .into_iter()
                    .into_group_map_by(|v| v.table_schema.clone());
                let schemas = schemas
                    .into_iter()
                    .map(|(k, v)| DbSchema {
                        db_schema_name: Some(k),
                        db_schema_tables: Some(
                            v.into_iter()
                                .map(|v| TableSchema {
                                    table_name: v.table_name,
                                    table_type: v.table_type,
                                    table_columns: None,
                                    table_constraints: None,
                                })
                                .collect(),
                        ),
                    })
                    .collect();

                Catalog {
                    catalog_name: Some(catalog_name),
                    catalog_db_schemas: Some(schemas),
                }
            })
            .collect();

        Ok(catalogs)
    }

    async fn fetch_min_schemas(&self, native_client: &NativeClient) -> Result<Vec<Catalog>> {
        let schemas = native_client
            .fetch_min_schemas(
                self.catalog_filter.map(|v| v.to_string()),
                self.schema_filter.map(|v| v.to_string()),
            )
            .await
            .map_err(|err| from_clickhouse_error("Failed to fetch schemas", err))?
            .collect_all()
            .await
            .map_err(|err| from_clickhouse_error("Failed to parse schemas", err))?;

        let catalogs = schemas
            .into_iter()
            .into_group_map_by(|v| v.catalog_name.clone())
            .into_iter()
            .map(|(catalog_name, schemas)| Catalog {
                catalog_name: Some(catalog_name),
                catalog_db_schemas: Some(
                    schemas
                        .into_iter()
                        .map(|s| DbSchema {
                            db_schema_name: Some(s.schema_name),
                            db_schema_tables: None,
                        })
                        .collect(),
                ),
            })
            .collect();

        Ok(catalogs)
    }

    async fn fetch_min_catalogs(&self, native_client: &NativeClient) -> Result<Vec<Catalog>> {
        let catalogs = native_client
            .fetch_min_schemas(self.catalog_filter.map(|v| v.to_string()), None)
            .await
            .map_err(|err| from_clickhouse_error("Failed to fetch catalogs", err))?
            .collect_all()
            .await
            .map_err(|err| from_clickhouse_error("Failed to parse catalogs", err))?;

        let catalogs = catalogs
            .into_iter()
            .map(|v| Catalog {
                catalog_name: Some(v.catalog_name),
                catalog_db_schemas: None,
            })
            .collect();

        Ok(catalogs)
    }
}
