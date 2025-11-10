# ADBC Driver for Clickhouse

## Example usage

```rust
use adbc_core::{
    Connection, Database, Driver,
    options::{OptionDatabase, OptionValue},
};

use adbc_clickhouse::driver::ClickhouseDriver;

fn main() {
    let mut driver = ClickhouseDriver::default();
    let database = driver
        .new_database_with_opts([
            (
                OptionDatabase::Uri,
                OptionValue::String("localhost:9000".to_string()),
            ),
            (
                OptionDatabase::Username,
                OptionValue::String("username".to_string()),
            ),
            (
                OptionDatabase::Password,
                OptionValue::String("password".to_string()),
            ),
            (
                OptionDatabase::Other("clickhouse.schema".to_string()),
                OptionValue::String("default".to_string()),
            )
        ])
        .unwrap();

    let connection = database.new_connection().unwrap();
    let batch = connection
        .get_objects(
            adbc_core::options::ObjectDepth::All,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap()
        .collect::<Vec<_>>()
        .into_iter()
        .filter_map(|s| s.ok())
        .collect::<Vec<_>>();

    arrow::util::pretty::print_batches(&batch).unwrap();
}
```
