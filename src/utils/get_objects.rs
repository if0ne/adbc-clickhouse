use std::sync::Arc;

use adbc_core::{error::Result, options::ObjectDepth, schemas};
use arrow_array::*;
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field};
use clickhouse_arrow::NativeClient;
use tokio::runtime::Runtime;

use crate::utils::{from_clickhouse_error, is_column_required, is_table_required};

use super::{ClickhouseResponseExt, NativeClientExt, is_schema_required};

pub(crate) struct GetObjectsBuilder<'a> {
    catalog_names: Vec<String>,
    catalog_db_schema_offsets: Vec<i32>,
    catalog_db_schema_offset_prev: i32,
    catalog_db_schema_names: Vec<String>,
    table_offsets: Vec<i32>,
    table_offset_prev: i32,
    table_names: Vec<String>,
    table_types: Vec<String>,
    column_offsets: Vec<i32>,
    column_offset_prev: i32,
    column_names: Vec<String>,
    column_types: Vec<String>,

    db_schema: &'a str,
    table_name: &'a str,
    column_name: &'a str,
}

impl<'a> GetObjectsBuilder<'a> {
    pub fn new(
        db_schema: Option<&'a str>,
        table_name: Option<&'a str>,
        column_name: Option<&'a str>,
    ) -> GetObjectsBuilder<'a> {
        GetObjectsBuilder {
            catalog_names: vec![],
            catalog_db_schema_offsets: vec![0],
            catalog_db_schema_offset_prev: 0,
            catalog_db_schema_names: vec![],
            table_offsets: vec![0],
            table_offset_prev: 0,
            table_names: vec![],
            table_types: vec![],
            column_offsets: vec![0],
            column_offset_prev: 0,
            column_names: vec![],
            column_types: vec![],

            db_schema: db_schema.unwrap_or("%"),
            table_name: table_name.unwrap_or("%"),
            column_name: column_name.unwrap_or("%"),
        }
    }

    pub fn build(
        mut self,
        runtime: &Runtime,
        native_client: &NativeClient,
        depth: &ObjectDepth,
    ) -> Result<RecordBatch> {
        self.catalog_names.push("default".to_string());

        if is_schema_required(depth) {
            let schemas = runtime.block_on(async {
                let response = native_client
                    .fetch_schemas(self.db_schema)
                    .await
                    .map_err(|err| from_clickhouse_error("Failed to fetch schemas", err))?;

                let schemas = response
                    .collect_all()
                    .await
                    .map_err(|err| from_clickhouse_error("Failed to fetch schemas", err))?;

                Result::Ok(schemas)
            })?;

            self.catalog_db_schema_names
                .extend(schemas.iter().map(|schema| schema.name.clone()));
            self.catalog_db_schema_offset_prev += schemas.len() as i32;
            self.catalog_db_schema_offsets
                .push(self.catalog_db_schema_offset_prev);

            if is_table_required(depth) {
                for schema in schemas {
                    let tables = runtime.block_on(async {
                        let response = native_client
                            .fetch_schema_tables(schema.name.clone(), self.table_name)
                            .await
                            .map_err(|err| from_clickhouse_error("Failed to fetch tables", err))?;

                        let tables = response
                            .collect_all()
                            .await
                            .map_err(|err| from_clickhouse_error("Failed to fetch tables", err))?;

                        Result::Ok(tables)
                    })?;

                    self.table_names
                        .extend(tables.iter().map(|s| s.name.to_string()));
                    self.table_offset_prev += tables.len() as i32;
                    self.table_offsets.push(self.table_offset_prev);

                    self.table_types
                        .extend((0..tables.len()).map(|_| "default".to_string()));

                    if is_column_required(depth) {
                        for table in tables {
                            let columns = runtime.block_on(async {
                                let response = native_client
                                    .fetch_table_columns(
                                        schema.name.clone(),
                                        table.name,
                                        self.column_name,
                                    )
                                    .await
                                    .map_err(|err| {
                                        from_clickhouse_error("Failed to fetch columns", err)
                                    })?;

                                let columns = response.collect_all().await.map_err(|err| {
                                    from_clickhouse_error("Failed to fetch columns", err)
                                })?;

                                Result::Ok(columns)
                            })?;

                            self.column_offset_prev += columns.len() as i32;
                            columns.into_iter().for_each(|f| {
                                self.column_names.push(f.name);
                                self.column_types.push(f.r#type);
                            });
                            self.column_offsets.push(self.column_offset_prev);
                        }
                    }
                }
            }
        }

        //////////////////////////////////////////////////////

        let table_columns_array = match depth {
            adbc_core::options::ObjectDepth::Columns | adbc_core::options::ObjectDepth::All => {
                let columns_struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("column_name", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.column_names.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("remarks", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
                        Arc::new(StringArray::from(self.column_types)) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
                        Arc::new(BooleanArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new(
                            "xdbc_is_generatedcolumn",
                            DataType::Boolean,
                            true,
                        )),
                        Arc::new(BooleanArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(self.column_offsets.clone())),
                    Arc::new(columns_struct_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                self.table_names.len(),
            ),
        };

        let db_schema_tables_array = match depth {
            adbc_core::options::ObjectDepth::Tables
            | adbc_core::options::ObjectDepth::Columns
            | adbc_core::options::ObjectDepth::All => {
                let table_constraints_array = ListArray::new_null(
                    Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
                    self.table_names.len(),
                );

                let tables_struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("table_name", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.table_names.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("table_type", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.table_types.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "table_columns",
                            Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(table_columns_array) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "table_constraints",
                            Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(table_constraints_array) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(self.table_offsets.clone())),
                    Arc::new(tables_struct_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                self.catalog_db_schema_names.len(),
            ),
        };

        let catalog_db_schemas_array = match depth {
            adbc_core::options::ObjectDepth::Columns
            | adbc_core::options::ObjectDepth::Tables
            | adbc_core::options::ObjectDepth::Schemas
            | adbc_core::options::ObjectDepth::All => {
                let db_schemas_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
                        Arc::new(StringArray::from(self.catalog_db_schema_names.clone()))
                            as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "db_schema_tables",
                            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(db_schema_tables_array) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new(
                        "item",
                        schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(),
                        true,
                    )),
                    OffsetBuffer::new(ScalarBuffer::from(self.catalog_db_schema_offsets.clone())),
                    Arc::new(db_schemas_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new(
                    "item",
                    schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(),
                    true,
                )),
                self.catalog_names.len(),
            ),
        };

        let catalog_name_array = StringArray::from(self.catalog_names);

        let batch = RecordBatch::try_new(
            schemas::GET_OBJECTS_SCHEMA.clone(),
            vec![
                Arc::new(catalog_name_array),
                Arc::new(catalog_db_schemas_array),
            ],
        )?;

        Ok(batch)
    }
}
