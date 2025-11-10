use std::sync::Arc;

use adbc_core::{
    Database, Optionable,
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
};

use crate::{connection::ClickhouseConnection, utils::from_clickhouse_error};

#[derive(Default)]
pub struct ClickhouseDatabase {
    uri: Option<String>,
    username: Option<String>,
    password: Option<String>,
}

impl Optionable for ClickhouseDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> Result<()> {
        let value = match value {
            OptionValue::String(value) => value,
            _ => {
                return Err(Error::with_message_and_status(
                    format!("[Clickhouse] Value is not a string, key: {key:?}"),
                    Status::InvalidArguments,
                ));
            }
        };

        match key {
            OptionDatabase::Uri => self.uri = Some(value),
            OptionDatabase::Username => self.username = Some(value),
            OptionDatabase::Password => self.password = Some(value),
            OptionDatabase::Other(_) => todo!(),
            _ => {
                return Err(Error::with_message_and_status(
                    format!("[Clickhouse] Unrecognized option: {key:?}"),
                    Status::NotFound,
                ));
            }
        }

        Ok(())
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        match key {
            OptionDatabase::Uri if self.uri.is_some() => Ok(self.uri.clone().unwrap()),
            OptionDatabase::Username if self.username.is_some() => {
                Ok(self.username.clone().unwrap())
            }
            OptionDatabase::Password if self.password.is_some() => {
                Ok(self.password.clone().unwrap())
            }
            _ => Err(Error::with_message_and_status(
                format!("[Clickhouse] Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        Err(Error::with_message_and_status(
            format!("[Clickhouse] Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }
}

impl Database for ClickhouseDatabase {
    type ConnectionType = ClickhouseConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(|err| {
                Error::with_message_and_status(
                    format!("[Clickhouse] Failed to create tokio runtime: {err}"),
                    Status::Internal,
                )
            })?;

        let uri = self.uri.clone();
        let username = self.username.clone();
        let password = self.password.clone();

        let builder = clickhouse_arrow::ClientBuilder::new().with_database("default");

        let builder = if let Some(uri) = uri {
            builder.with_endpoint(uri)
        } else {
            builder
        };

        let builder = if let Some(username) = username {
            builder.with_username(username)
        } else {
            builder
        };

        let builder = if let Some(password) = password {
            builder.with_password(password)
        } else {
            builder
        };

        let arrow_builder = builder.clone();
        let arrow_conn = rt
            .block_on(async move { arrow_builder.build_arrow().await })
            .map_err(|err| {
                from_clickhouse_error("[Clickhouse] Failed to create arrow clickhouse client", err)
            })?;

        let native_conn = rt
            .block_on(async move { builder.build_native().await })
            .map_err(|err| {
                from_clickhouse_error(
                    "[Clickhouse] Failed to create native clickhouse client",
                    err,
                )
            })?;

        Ok(ClickhouseConnection::new(
            Arc::new(rt),
            arrow_conn,
            native_conn,
        ))
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionConnection,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> Result<Self::ConnectionType> {
        let mut connection = self.new_connection()?;

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}
