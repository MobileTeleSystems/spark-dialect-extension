## 0.0.2 (2024-10-02)

* Allow writing `ArrayType(TimestampType())` Spark column as Clickhouse's `Array(DateTime64(6))`.
* Allow writing `ArrayType(ShortType())` Spark column as Clickhouse's `Array(Int16)`.

## 0.0.1 (2024-10-01)

First release! 🎉

This version includes custom Clickhouse dialect for Apache Spark 3.5.x, with following enhancements:
* support for writing Spark's `ArrayType` to Clickhouse. Currently [only few types](https://github.com/ClickHouse/clickhouse-java/issues/1754) are supported, like `ArrayType(StringType)`, `ArrayType(ByteType)`, `ArrayType(LongType)`, `ArrayType(FloatType)`. Unfortunately, reading Arrays from Clickhouse to Spark is not fully supported for now.
* fixed issue when writing Spark's `TimestampType` lead to creating Clickhouse table with `DateTime64(0)` instead of `DateTime64(6)`, resulting a precision loss (fractions of seconds were dropped).
* fixed issue when writing Spark's `BooleanType` lead to creating Clickhouse table with `UInt64` column instead of `Bool`.
