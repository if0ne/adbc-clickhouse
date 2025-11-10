use std::{pin::Pin, sync::Arc};

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::{ArrowError, Schema};
use clickhouse_arrow::ClickHouseResponse;
use futures::{StreamExt, stream::Peekable};

use crate::utils::Runtime;

#[derive(Debug)]
pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: Arc<Schema>,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }
}

pub struct ClickhouseReader {
    rt: Arc<Runtime>,
    stream: Pin<Box<Peekable<ClickHouseResponse<RecordBatch>>>>,
    schema: Option<Arc<arrow_schema::Schema>>,
}

impl ClickhouseReader {
    pub fn new(rt: Arc<Runtime>, stream: ClickHouseResponse<RecordBatch>) -> Self {
        let mut peekable = Box::pin(stream.peekable());
        let schema = rt
            .block_on(peekable.as_mut().peek())
            .map(|res| res.as_ref().map(|res| res.schema()))
            .transpose()
            .ok()
            .flatten();

        Self {
            rt,
            stream: peekable,
            schema,
        }
    }
}

impl Iterator for ClickhouseReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.rt.block_on(self.stream.next())?;
        Some(next.map_err(|err| ArrowError::ExternalError(Box::new(err))))
    }
}

impl RecordBatchReader for ClickhouseReader {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone().expect("failed to fetch schema")
    }
}
