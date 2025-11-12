use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Catalog {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub catalog_db_schemas: Option<Vec<DbSchema>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DbSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_schema_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_schema_tables: Option<Vec<TableSchema>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TableSchema {
    pub table_name: String,
    pub table_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_columns: Option<Vec<ColumnSchema>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_constraints: Option<Vec<ConstraintSchema>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub column_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ordinal_position: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub remarks: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_data_type: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_type_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_column_size: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_decimal_digits: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_num_prec_radix: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_nullable: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_column_def: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_sql_data_type: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_datetime_sub: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_char_octet_length: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_is_nullable: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_scope_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_scope_schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_scope_table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_is_autoincrement: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub xdbc_is_generatedcolumn: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConstraintSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_name: Option<String>,
    pub constraint_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_column_names: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub constraint_column_usage: Option<Vec<UsageSchema>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UsageSchema {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fk_catalog: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fk_db_schema: Option<String>,
    pub fk_table: String,
    pub fk_column_name: String,
}
