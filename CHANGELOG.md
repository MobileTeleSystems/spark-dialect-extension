## 0.0.6 (unreleased)

### Features

Added support for Apache Spark 4.x on Scala 2.13, published as `spark-dialect-extension_2.13`
alongside the existing `spark-dialect-extension_2.12` for Spark 3.5.x (Scala 2.12). The build is
cross-built via `-PsparkProfile=spark3|spark4`; the correct dialect implementation is selected at
runtime from the Spark version, so a single Scala 2.13 artifact covers every Spark 4.x minor
release. Spark 4 requires Java 17.

Spark 4's `JdbcDialect.isObjectNotFoundException` is overridden to recognise ClickHouse error code
60 (`UNKNOWN_TABLE`) via the JDBC `SQLException` vendor code, so writing to a not-yet-existing
table works. This is locale-independent (does not depend on the server error message text).

## 0.0.5 (2026-08-03)

### Improvements

On Clickhouse JDBC 0.9.x types `UUID`, `IPv4`, `IPv6` are threated as Spark `StringType()` instead of raising an exception.

## 0.0.4 (2026-04-07)

### Features

Added support for `df.write.format("jdbc").option("truncate", "true")`

### Improvements

Added tests for Clickhouse JDBC 0.9.5+.

This JDBC driver version allows using `Array(T)` for almost all `T`, including `Float32`, `Date`, `DateTime` and `Decimal`, see https://github.com/ClickHouse/clickhouse-java/pull/2627.
Except for `UInt64` - there is an issue on Spark side.

### Bug fixes

Convert `UInt64` to `Decimalype(38, 0)` instead of `DecimalType(20, 0)` (Spark's default).

## 0.0.3 (2025-10-31)

### Features

* Added support for Clickhouse JDBC 0.9.2+.

  This allows using `Array(T)` for numeric `T`, like `Int16`, `Int32`, `Int64`, `Float64`.

  But `Float32`, `Date`, `DateTime` and `Decimal` are not supported, see [issue](https://github.com/ClickHouse/clickhouse-java/issues/2457).

* Wrap with `Nullable(T)` Spark DataFrame columns with `nullable = true`.
  
  Caveat - Spark DataFrames created from ORC and Parquet files have all columns with `nullable = true`.
  Using:
  
  ```python
  df.write.format("jdbc").option("createTableOptions", "ENGINE = ReplacingMergeTree() ORDER BY (col1)")
  ```
  
  will fail if `col1` is nullable. Workaround:
  
  ```python
  import pyspark.sql.functions as F

  # make column non-nullable with coalesce
  # F.lit(...) should contain value compatible with `col1` type
  df = df.withColumn("a", F.coalesce("a", F.lit(0)))
  ```

## 0.0.2 (2024-10-02)

### Features

* Allow writing `ArrayType(TimestampType())` Spark column as Clickhouse's `Array(DateTime64(6))`.
* Allow writing `ArrayType(ShortType())` Spark column as Clickhouse's `Array(Int16)`.

## 0.0.1 (2024-10-01)

First release! 🎉

This version includes custom Clickhouse dialect for Apache Spark 3.5.x, with following features:
* support for writing Spark's `ArrayType` to Clickhouse. Currently [only few types](https://github.com/ClickHouse/clickhouse-java/issues/1754) are supported, like `ArrayType(StringType)`, `ArrayType(ByteType)`, `ArrayType(LongType)`, `ArrayType(FloatType)`. Unfortunately, reading Arrays from Clickhouse to Spark is not fully supported for now.
* fixed issue when writing Spark's `TimestampType` lead to creating Clickhouse table with `DateTime64(0)` instead of `DateTime64(6)`, resulting a precision loss (fractions of seconds were dropped).
* fixed issue when writing Spark's `BooleanType` lead to creating Clickhouse table with `UInt64` column instead of `Bool`.
