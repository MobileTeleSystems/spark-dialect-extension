## Data Type Mappings for Spark Dialect Extension

This documentation outlines the customized mappings that the Spark Dialect Extension implements that optimize interactions between Spark and ClickHouse.

#### Customized Type Mappings with Spark Dialect Extension

| ClickHouse Type (Read)     | Spark Type                     | ClickHouse Type (Write)       | ClickHouse Type (Create)    |
|----------------------------|--------------------------------|-------------------------------|-----------------------------|
| `Int8`                     | `ByteType`                     | `Int8`                        | `Int8`                      |
| `Int16`                    | `ShortType`                    | `Int16`                       | `Int16`                     |
| `Datetime64(6)`            | `TimestampType`                | `Datetime64(6)`               | `Datetime64(6)`             |
| `Bool`                     | `BooleanType`                  | `Bool`                        | `Bool`                      |


#### Default Type Mappings without Spark Dialect Extension

| ClickHouse Type (Read)     | Spark Type                     | ClickHouse Type (Write)       | ClickHouse Type (Create)    |
|----------------------------|--------------------------------|-------------------------------|-----------------------------|
| `Int8`                     | `IntegerType`                  | `Int32`                       | `Int32`                     |
| `Int16`                    | `IntegerType`                  | `Int32`                       | `Int32`                     |
| `Datetime64(6)`            | `TimestampType`                | `Datetime64(6)`               | `DateTime32`                |
| `Bool`                     | `BooleanType`                  | `Bool`                        | `UInt64`                    |
