## Data Type Mappings for Spark Dialect Extension

This documentation outlines the customized mappings that the Spark Dialect Extension implements that optimize interactions between Spark and ClickHouse.

#### Customized Type Mappings with Spark Dialect Extension

| ClickHouse Type (Read)     | Spark Type                     | ClickHouse Type (Write)       | ClickHouse Type (Create)    |
|----------------------------|--------------------------------|-------------------------------|-----------------------------|
| `Int8`                     | `ByteType`                     | `Int8`                        | `Int8`                      |
| `Int16`                    | `ShortType`                    | `Int16`                       | `Int16`                     |
| `Datetime64(6)`            | `TimestampType`                | `Datetime64(6)`               | `Datetime64(6)`             |
| `Bool`                     | `BooleanType`                  | `Bool`                        | `Bool`                      |

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

| ClickHouse Type (Read)     | Spark Type                     | ClickHouse Type (Write)       | ClickHouse Type (Create)    |
|----------------------------|--------------------------------|-------------------------------|-----------------------------|
| `Int8`                     | `IntegerType`                  | `Int32`                       | `Int32`                     |
| `Int16`                    | `IntegerType`                  | `Int32`                       | `Int32`                     |
| `Datetime64(6)`            | `TimestampType`                | `Datetime64(6)`               | `DateTime32`                |
| `Bool`                     | `BooleanType`                  | `Bool`                        | `UInt64`                    |

``Array(T)`` `->` ``ArrayType(T)``:

**unsupported**