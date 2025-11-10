use std::sync::Arc;

use adbc_core::{error::Result, options::InfoCode, schemas};
use arrow_array::{RecordBatch, UnionArray, builder::*};
use arrow_buffer::ScalarBuffer;
use arrow_schema::DataType;

pub(crate) struct GetInfoBuilder {
    name_builder: UInt32Builder,
    type_id_vec: Vec<i8>,
    offsets_vec: Vec<i32>,
    string_array_builder: StringBuilder,
    string_offset: i32,
    bool_array_builder: BooleanBuilder,
    int64_array_builder: Int64Builder,
    int64_offset: i32,
    int32_array_builder: Int32Builder,
    list_string_array_builder: ListBuilder<StringBuilder>,
    map_builder: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
}

impl GetInfoBuilder {
    pub fn new() -> GetInfoBuilder {
        GetInfoBuilder {
            name_builder: UInt32Builder::new(),
            type_id_vec: vec![],
            offsets_vec: vec![],
            string_array_builder: StringBuilder::new(),
            string_offset: 0,
            bool_array_builder: BooleanBuilder::new(),
            int64_array_builder: Int64Builder::new(),
            int64_offset: 0,
            int32_array_builder: Int32Builder::new(),
            list_string_array_builder: ListBuilder::new(StringBuilder::new()),
            map_builder: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
        }
    }

    pub fn set_string(&mut self, code: InfoCode, string: &str) {
        self.name_builder.append_value(Into::<u32>::into(&code));
        self.string_array_builder.append_value(string);
        self.type_id_vec.push(0);
        self.offsets_vec.push(self.string_offset);
        self.string_offset += 1;
    }

    pub fn set_number(&mut self, code: InfoCode, number: i64) {
        self.name_builder.append_value(Into::<u32>::into(&code));
        self.int64_array_builder.append_value(number);
        self.type_id_vec.push(2);
        self.offsets_vec.push(self.int64_offset);
        self.int64_offset += 1;
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let fields = match schemas::GET_INFO_SCHEMA
            .field_with_name("info_value")
            .unwrap()
            .data_type()
        {
            DataType::Union(fields, _) => Some(fields),
            _ => None,
        };

        let value_array = UnionArray::try_new(
            fields.unwrap().clone(),
            self.type_id_vec.into_iter().collect::<ScalarBuffer<i8>>(),
            Some(self.offsets_vec.into_iter().collect::<ScalarBuffer<i32>>()),
            vec![
                Arc::new(self.string_array_builder.finish()),
                Arc::new(self.bool_array_builder.finish()),
                Arc::new(self.int64_array_builder.finish()),
                Arc::new(self.int32_array_builder.finish()),
                Arc::new(self.list_string_array_builder.finish()),
                Arc::new(self.map_builder.finish()),
            ],
        )?;

        Ok(RecordBatch::try_new(
            schemas::GET_INFO_SCHEMA.clone(),
            vec![Arc::new(self.name_builder.finish()), Arc::new(value_array)],
        )?)
    }
}

impl Default for GetInfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}
