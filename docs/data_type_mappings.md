## Data Type Mappings for Spark Dialect Extension

This documentation outlines the customized mappings that the Spark Dialect Extension implements that optimize interactions between Spark and ClickHouse.

| ClickHouse Type (Read)     | Spark Type                     | ClickHouse Type (Write)       | ClickHouse Type (Create)    |
|----------------------------|--------------------------------|-------------------------------|-----------------------------|
| `Int8`                     | `ByteType`                     | `Int8`                        | `Int8`                      |
| `Int16`                    | `ShortType`                    | `Int16`                       | `Int16`                     |
| `Datetime64(6)`            | `TimestampType`                | `Datetime64(6)`               | `Datetime64(6)`             |
| `Bool`                     | `BooleanType`                  | `Bool`                        | `Bool`                      |

For a reference to the default mappings, visit [Default Type Mappings](https://onetl.readthedocs.io/en/stable/connection/db_connection/clickhouse/types.html).
