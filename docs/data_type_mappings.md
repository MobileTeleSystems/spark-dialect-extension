## Data Type Mappings for Spark Dialect Extension

This documentation outlines the customized mappings that the Spark Dialect Extension implements that optimize interactions between Spark and ClickHouse.

#### Customized Type Mappings with Spark Dialect Extension

| ClickHouse Type (Read) | Spark Type           | ClickHouse Type (Write) | ClickHouse Type (Create) |
|------------------------|----------------------|-------------------------|--------------------------|
| `Bool`                 | `BooleanType`        | `Bool`                  | `Bool`                   |
| `Int8`                 | `ByteType`           | `Int8`                  | `Int8`                   |
| `Int16`                | `ShortType`          | `Int16`                 | `Int16`                  |
| `Int32`                | `IntegerType`        | `Int32`                 | `Int32`                  |
| `Int64`                | `LongType`           | `Int64`                 | `Int64`                  |
| `UInt8`                | `ShortType`          | `UInt8`                 | `UInt8`                  |
| `UInt16`               | `IntegerType`        | `UInt16`                | `UInt16`                 |
| `UInt32`               | `LongType`           | `Int64`                 | `Int64`                  |
| `UInt64`               | `DecimalType(20, 0)` | `Decimal(20, 0)`        | `Decimal(20, 0)`         |
| `Float32`              | `FloatType`          | `Float32`               | `Float32`                |
| `Float64`              | `DoubleType`         | `Float64`               | `Float64`                |
| `Decimal(M, N)`        | `DecimalType(M, N)`  | `Decimal(M, N)`         | `Decimal(M, N)`          |
| `Decimal32(N)`         | `DecimalType(M, N)`  | `Decimal32(M, N)`       | `Decimal32(M, N)`        |
| `Decimal64(N)`         | `DecimalType(M, N)`  | `Decimal64(M, N)`       | `Decimal64(M, N)`        |
| `Decimal128(N)`        | `DecimalType(M, N)`  | `Decimal128(M, N)`      | `Decimal128(M, N)`       |
| `Decimal256(N)`        | unsupported          | unsupported             | unsupported              |
| `DateTime`             | `TimestampType`      | `DateTime`              | `DateTime`               |
| `Datetime64(6)`        | `TimestampType`      | `Datetime64(6)`         | `Datetime64(6)`          |


``Array(T)`` `->` ``ArrayType(T)``:

| ClickHouse Type (Read) | Spark Type                     | ClickHouse Type (Write) | ClickHouse Type (Create) |
|------------------------|--------------------------------|-------------------------|--------------------------|
| `Array(String)`        | `ArrayType(StringType)`        | `Array(String)`         | `Array(String)`          |
| unsupported            | `ArrayType(ByteType)`          | `Array(Int8)`           | `Array(Int8)`            |
| unsupported            | `ArrayType(ShortType)`         | unsupported             | unsupported              |
| unsupported            | `ArrayType(LongType)`          | `Array(Int64)`          | `Array(Int64)`           |
| `Array(Decimal(M, N))` | `ArrayType(DecimalType(M, N))` | `Array(Decimal(M, N))`  | `Array(Decimal(M, N))`   |
| unsupported            | `ArrayType(TimestampType)`     | unsupported             | unsupported              |
| unsupported            | `ArrayType(Date)`              | `Array(Date)`           | `Array(Date)`            |
| unsupported            | `ArrayType(FloatType)`         | `Array(Float32)`        | `Array(Float32)`         |
| unsupported            | `ArrayType(DoubleType)`        | unsupported             | unsupported              |


#### Default Type Mappings without Spark Dialect Extension

| ClickHouse Type (Read) | Spark Type           | ClickHouse Type (Write) | ClickHouse Type (Create) |
|------------------------|----------------------|-------------------------|--------------------------|
| `Bool`                 | `BooleanType`        | `Bool`                  | `UInt64`                 |
| `Int8`                 | `IntegerType`        | `Int32`                 | `Int32`                  |
| `Int16`                | `IntegerType`        | `Int32`                 | `Int32`                  |
| `Int32`                | `IntegerType`        | `Int32`                 | `Int32`                  |
| `Int64`                | `LongType`           | `Int64`                 | `Int64`                  |
| `UInt8`                | `IntegerType`        | `UInt8`                 | `UInt8`                  |
| `UInt16`               | `IntegerType`        | `UInt16`                | `UInt16`                 |
| `UInt32`               | `DecimalType(20, 0)` | `Decimal(20, 0)`        | `Decimal(20, 0)`         |
| `UInt64`               | `DecimalType(20, 0)` | `Decimal(20, 0)`        | `Decimal(20, 0)`         |
| `Float32`              | `FloatType`          | `Float32`               | `Float32`                |
| `Float64`              | `DoubleType`         | `Float64`               | `Float64`                |
| `Decimal(M, N)`        | `DecimalType(M, N)`  | `Decimal(M, N)`         | `Decimal(M, N)`          |
| `Decimal32(N)`         | `DecimalType(M, N)`  | `Decimal32(M, N)`       | `Decimal32(M, N)`        |
| `Decimal64(N)`         | `DecimalType(M, N)`  | `Decimal64(M, N)`       | `Decimal64(M, N)`        |
| `Decimal128(N)`        | `DecimalType(M, N)`  | `Decimal128(M, N)`      | `Decimal128(M, N)`       |
| `Decimal256(N)`        | unsupported          | unsupported             | unsupported              |
| `DateTime`             | `TimestampType`      | `DateTime`              | `DateTime`               |
| `Datetime64(6)`        | `TimestampType`      | `Datetime64(6)`         | `DateTime32`             |

``Array(T)`` `->` ``ArrayType(T)``:

**unsupported**