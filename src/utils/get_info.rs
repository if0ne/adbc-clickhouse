use crate::{InfoEntry, InfoValue};
use adbc_core::{error::Result, options::InfoCode, schemas};
use arrow_array::RecordBatch;

pub(crate) struct GetInfoBuilder {
    entries: Vec<InfoEntry>,
}

impl GetInfoBuilder {
    pub fn new() -> GetInfoBuilder {
        GetInfoBuilder {
            entries: Default::default(),
        }
    }

    pub fn set_string(&mut self, code: InfoCode, string: impl Into<String>) {
        self.entries.push(InfoEntry {
            info_name: Into::<u32>::into(&code),
            info_value: Some(InfoValue::StringValue(Some(string.into()))),
        });
    }

    pub fn set_number(&mut self, code: InfoCode, number: i64) {
        self.entries.push(InfoEntry {
            info_name: Into::<u32>::into(&code),
            info_value: Some(InfoValue::Int64Value(Some(number))),
        });
    }

    pub fn finish(self) -> Result<RecordBatch> {
        let record_batch =
            serde_arrow::to_record_batch(schemas::GET_INFO_SCHEMA.fields(), &self.entries)
                .map_err(|err| {
                    adbc_core::error::Error::with_message_and_status(
                        format!("Failed to serialize catalogs: {err}"),
                        adbc_core::error::Status::Internal,
                    )
                })?;

        Ok(record_batch)
    }
}

impl Default for GetInfoBuilder {
    fn default() -> Self {
        Self::new()
    }
}
