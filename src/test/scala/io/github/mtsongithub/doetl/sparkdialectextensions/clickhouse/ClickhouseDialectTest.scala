package io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse

import io.github.mtsongithub.doetl.sparkdialectextensions.SharedSparkSession
import org.mockito.Mockito._
import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

class ClickhouseDialectTest
    extends AnyFunSuite
    with Matchers
    with SharedSparkSession
    with TableDrivenPropertyChecks
    with ClickhouseFixture {

  /**
   * this function is used to normalize schemas for comparison purposes to handle cases such as:
   * 'StructField("shortColumn", ShortType, true, {"scale":0})' is equal to
   * 'StructField("shortColumn", ShortType, true, {})'
   */
  def stripMetadata(schema: StructType): StructType = {
    StructType(schema.fields.map(f => f.copy(metadata = Metadata.empty)))
  }

  test("custom clickhouse dialect usage") {
    val dialect = JdbcDialects.get("jdbc:clickhouse")
    assert(dialect.getClass.getName.contains("ClickhouseDialectExtension"))
  }

  test("throws exception for unsupported spark version") {
    val originalSession = SparkSession.getActiveSession

    // mock the SparkSession to return version 3.4
    val mockSparkSession = mock(classOf[SparkSession])
    when(mockSparkSession.version).thenReturn("3.4.0")
    SparkSession.setActiveSession(mockSparkSession)

    try {
      val exception = intercept[UnsupportedOperationException] {
        ClickhouseDialectRegistry.register()
      }
      // check the exception message
      exception.getMessage should include("Unsupported Spark version: 3.4.0")
    } finally {
      // restore the original session after the test
      SparkSession.setActiveSession(originalSession.orNull)
    }
  }

  test("test clickHouse table read with spark") {

    val schema =
      """
        |booleanColumn Boolean,
        |byteColumn Int8,
        |shortColumn Int16,
        |timestampColumn Datetime64(6),
    """.stripMargin
    setupTable(schema)

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    val expectedSchema = StructType(
      Seq(
        StructField("booleanColumn", BooleanType, nullable = true),
        StructField("byteColumn", ByteType, nullable = true),
        StructField("shortColumn", ShortType, nullable = true),
        StructField("timestampColumn", TimestampType, nullable = true)))

    assert(df.schema.treeString === expectedSchema.treeString)
  }

  test("test spark dataframe write to existing clickhouse table") {

    val schema =
      """
        |booleanColumn Boolean,
        |byteColumn Int8,
        |shortColumn Int16,
        |timestampColumn Datetime64(6),
    """.stripMargin
    setupTable(schema)

    val generator = new ClickhouseDataframeGenerator(spark)
    val df = generator.createDataFrame()

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .mode("append")
      .save()

    val loadedDf = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(stripMetadata(loadedDf.schema) == stripMetadata(df.schema))
  }

  test("test spark dataframe write to new clickhouse table") {

    val generator = new ClickhouseDataframeGenerator(spark)
    val df = generator.createDataFrame()

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("createTableOptions", "ENGINE = TinyLog")
      .mode("errorIfExists")
      .save()

    val loadedDf = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(stripMetadata(loadedDf.schema) == stripMetadata(df.schema))
  }

  test("read ClickHouse Int8 as Spark ByteType") {
    setupTable("byteColumn Int8")
    insertTestData(Seq("(-128)", "(127)")) // min and max values for a signed byte

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == ByteType)

    val data = df.collect().map(_.getByte(0)).sorted
    assert(data sameElements Array(-128, 127))
  }

  test("read ClickHouse Int16 as Spark ShortType") {
    setupTable("shortColumn Int16")
    insertTestData(Seq("(-32768)", "(32767)")) // min and max values for a signed short

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == ShortType)

    val data = df.collect().map(_.getShort(0)).sorted
    assert(data sameElements Array(-32768, 32767))
  }

  test("write Spark ShortType as ClickHouse Int16") {
    val schema = StructType(Seq(StructField("shortColumn", ShortType, nullable = true)))
    val data = Seq(Row(Short.MinValue), Row(Short.MaxValue))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("createTableOptions", "ENGINE = TinyLog")
      .mode("errorIfExists")
      .save()

    val statement = connection.createStatement()
    val rs = statement.executeQuery(s"DESCRIBE TABLE $tableName")
    while (rs.next()) {
      val columnName = rs.getString("name")
      val dataType = rs.getString("type")
      columnName match {
        case "shortColumn" => assert(dataType == "Int16")
      }
    }
    statement.close()
  }

  test("read ClickHouse UInt8 as Spark ShortType") {
    setupTable("uByteColumn UInt8")
    insertTestData(Seq("(0)", "(255)")) // min and max values for unsigned byte

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == ShortType)

    val data = df.collect().map(_.getShort(0)).sorted
    assert(data sameElements Array(0, 255))
  }

  test("read ClickHouse UInt16 as Spark IntegerType") {
    setupTable("uShortColumn UInt16")
    insertTestData(Seq("(0)", "(65535)")) // min and max values for unsigned short

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == IntegerType)

    val data = df.collect().map(_.getInt(0)).sorted
    assert(data sameElements Array(0, 65535))
  }

  test("read ClickHouse UInt32 as Spark LongType") {
    setupTable("uIntColumn UInt32")
    insertTestData(Seq("(0)", "(4294967295)")) // min and max values for unsigned int

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == LongType)

    val data = df.collect().map(_.getLong(0)).sorted
    assert(data sameElements Array(0L, 4294967295L))
  }

  test("read ClickHouse Float32 as Spark FloatType") {
    setupTable("floatColumn Float32")
    insertTestData(Seq("(-1.23)", "(4.56)"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == FloatType)
    val data = df.collect().map(_.getFloat(0)).sorted
    assert(data sameElements Array(-1.23f, 4.56f))
  }

  test("read ClickHouse Float64 as Spark DoubleType") {
    setupTable("doubleColumn Float64")
    insertTestData(
      Seq("(-1.7976931348623157E308)", "(1.7976931348623157E308)")
    ) // min and max values for double

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DoubleType)

    val data = df.collect().map(_.getDouble(0)).sorted
    assert(data sameElements Array(-1.7976931348623157e308, 1.7976931348623157e308))
  }

  test("read ClickHouse Decimal(9,2) as Spark DecimalType") {
    setupTable("decimalColumn Decimal(9,2)")
    insertTestData(Seq("(12345.67)", "(89012.34)"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DecimalType(9, 2))

    val data = df.collect().map(_.getDecimal(0)).sorted
    assert(
      data sameElements Array(
        new java.math.BigDecimal("12345.67"),
        new java.math.BigDecimal("89012.34")))
  }

  test("read ClickHouse Decimal32 as Spark DecimalType(9, scale)") {
    setupTable("decimalColumn Decimal32(2)")
    insertTestData(Seq("(12345.67)", "(89012.34)"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DecimalType(9, 2))

    val data = df.collect().map(_.getDecimal(0)).sorted
    assert(
      data sameElements Array(
        new java.math.BigDecimal("12345.67"),
        new java.math.BigDecimal("89012.34")))
  }

  test("read ClickHouse Decimal64 as Spark DecimalType(18, scale)") {
    setupTable("decimalColumn Decimal64(2)")
    insertTestData(Seq("(123456789.12)", "(987654321.34)"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DecimalType(18, 2))

    val data = df.collect().map(_.getDecimal(0)).sorted
    assert(
      data sameElements Array(
        new java.math.BigDecimal("123456789.12"),
        new java.math.BigDecimal("987654321.34")))
  }

  test("read ClickHouse Decimal128 as Spark DecimalType(38, scale)") {
    setupTable("decimalColumn Decimal128(2)")
    insertTestData(
      Seq("(123456789012345678901234567890.12)", "(987654321098765432109876543210.34)"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DecimalType(38, 2))

    val data = df.collect().map(_.getDecimal(0)).sorted
    assert(
      data sameElements Array(
        new java.math.BigDecimal("123456789012345678901234567890.12"),
        new java.math.BigDecimal("987654321098765432109876543210.34")))
  }

  test("read ClickHouse Date as Spark DateType") {
    setupTable("dateColumn Date")
    insertTestData(Seq("('2023-01-01')", "('2023-12-31')"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == DateType)

    val data = df.collect().map(_.getDate(0))
    assert(
      data sameElements Array(
        java.sql.Date.valueOf("2023-01-01"),
        java.sql.Date.valueOf("2023-12-31")))
  }

  test("read ClickHouse DateTime as Spark TimestampType") {
    setupTable("datetimeColumn DateTime")
    insertTestData(Seq("('2023-01-01 12:34:56')", "('2023-12-31 23:59:59')"))

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.schema.fields.head.dataType == TimestampType)

    val data = df.collect().map(_.getTimestamp(0))
    assert(
      data sameElements Array(
        java.sql.Timestamp.valueOf("2023-01-01 12:34:56"),
        java.sql.Timestamp.valueOf("2023-12-31 23:59:59")))
  }

  test("write Spark TimestampType as ClickHouse Datetime64(6)") {
    val schema = StructType(Seq(StructField("timestampColumn", TimestampType, nullable = true)))
    val currentTime = new java.sql.Timestamp(System.currentTimeMillis())
    val data = Seq(Row(currentTime))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("createTableOptions", "ENGINE = TinyLog")
      .mode("errorIfExists")
      .save()

    val statement = connection.createStatement()
    val rs = statement.executeQuery(s"DESCRIBE TABLE $tableName")
    while (rs.next()) {
      val columnName = rs.getString("name")
      val dataType = rs.getString("type")
      columnName match {
        case "timestampColumn" => assert(dataType == "DateTime64(6)")
      }
    }
    statement.close()
  }

  test("write Spark BooleanType as ClickHouse Bool") {
    val schema = StructType(Seq(StructField("booleanColumn", BooleanType, nullable = true)))
    val data = Seq(Row(true), Row(false))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .option("createTableOptions", "ENGINE = TinyLog")
      .mode("errorIfExists")
      .save()

    val statement = connection.createStatement()
    val rs = statement.executeQuery(s"DESCRIBE TABLE $tableName")
    while (rs.next()) {
      val columnName = rs.getString("name")
      val dataType = rs.getString("type")
      columnName match {
        case "booleanColumn" => assert(dataType == "Bool")
      }
    }
    statement.close()
  }

  val testReadArrayCases = Table(
    ("columnDefinition", "insertedData", "expectedType"),
    (
      "charArrayColumn Array(String)",
      "(['a', 'b', 'c', 'd', 'e'])",
      ArrayType(StringType, containsNull = false)),
    (
      "decimalArrayColumn Array(Decimal(9,2))",
      "([1.23, 2.34, 3.45, 4.56, 5.67])",
      ArrayType(DecimalType(9, 2), containsNull = false)))

  forAll(testReadArrayCases) {
    (columnDefinition: String, insertedData: String, expectedType: DataType) =>
      test(s"read ClickHouse Array for ${columnDefinition} column") {
        setupTable(columnDefinition)
        insertTestData(Seq(insertedData))

        val df = spark.read
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", tableName)
          .load()

        assert(df.schema.fields.head.dataType === expectedType)

        df.collect().map(_.getAs[Seq[Any]](df.schema.fields.head.name)).head
        assert(df.count() == 1)
      }
  }

  val testReadArrayUnsupportedCases = Table(
    ("columnDefinition", "insertedData", "expectedType", "errorMessage"),
    // https://github.com/ClickHouse/clickhouse-java/issues/1754
    (
      "byteArrayColumn Array(Int8)",
      "([1, 2, 3, 4, 5])",
      ArrayType(ByteType, containsNull = false),
      "class [B cannot be cast to class [Ljava.lang.Object;"),
    (
      "shortArrayColumn Array(Int16)",
      "([1, 2, 3, 4, 5])",
      ArrayType(ShortType, containsNull = false),
      "class [S cannot be cast to class [Ljava.lang.Object;"),
    (
      "intArrayColumn Array(Int32)",
      "([1, 2, 3, 4, 5])",
      ArrayType(IntegerType, containsNull = false),
      "class [I cannot be cast to class [Ljava.lang.Object;"),
    (
      "longArrayColumn Array(Int64)",
      "([1, 2, 3, 4, 5])",
      ArrayType(LongType, containsNull = false),
      "class [J cannot be cast to class [Ljava.lang.Object"),
    // https://github.com/ClickHouse/clickhouse-java/issues/1409
    (
      "dateArrayColumn Array(Date)",
      "(['2024-01-01', '2024-01-02', '2024-01-03'])",
      ArrayType(DateType, containsNull = false),
      "class [Ljava.time.LocalDate; cannot be cast to class [Ljava.sql.Date;"),
    (
      "datetimeArrayColumn Array(DateTime64(6))",
      "(['2024-01-01T00:00:00.000000', '2024-01-02T11:11:11.111111', '2024-01-03.2222222'])",
      ArrayType(TimestampType, containsNull = false),
      "class [Ljava.time.LocalDateTime; cannot be cast to class [Ljava.sql.Timestamp;"))

  forAll(testReadArrayUnsupportedCases) {
    (
        columnDefinition: String,
        insertedData: String,
        expectedType: DataType,
        errorMessage: String) =>
      test(s"cannot read ClickHouse Array for ${columnDefinition} column") {
        setupTable(columnDefinition)
        insertTestData(Seq(insertedData))

        // schema is detected properly
        val df = spark.read
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", tableName)
          .load()

        assert(df.schema.fields.head.dataType === expectedType)

        // but read is failing
        val exception = intercept[SparkException] {
          df.collect()
        }
        // check the exception message
        exception.getMessage should include(errorMessage)
      }
  }

  val testWriteArrayCases = Table(
    ("columnName", "insertedData", "expectedType", "expectedClickhouseType"),
    (
      "charArrayColumn",
      Seq(Row(Array("a", "b", "c", "d", "e"))),
      ArrayType(StringType, containsNull = false),
      "Array(String)"),
    (
      "byteArrayColumn",
      Seq(Row(Array(1.toByte, 2.toByte, 3.toByte, 4.toByte, 5.toByte))),
      ArrayType(ByteType, containsNull = false),
      "Array(Int8)"),
    (
      "shortArrayColumn",
      Seq(Row(Array(1.toShort, 2.toShort, 3.toShort, 4.toShort, 5.toShort))),
      ArrayType(ShortType, containsNull = false),
      "Array(Int16)"),
    (
      "intArrayColumn",
      Seq(Row(Array(1, 2, 3, 4, 5))),
      ArrayType(IntegerType, containsNull = false),
      "Array(Int32)"),
    (
      "longArrayColumn",
      Seq(Row(Array(1L, 2L, 3L, 4L, 5L))),
      ArrayType(LongType, containsNull = false),
      "Array(Int64)"),
    (
      "floatArrayColumn",
      Seq(Row(Array(1.0f, 2.0f, 3.0f, 4.0f, 5.0f))),
      ArrayType(FloatType, containsNull = false),
      "Array(Float32)"),
    (
      "doubleArrayColumn",
      Seq(Row(Array(1.0d, 2.0d, 3.0d, 4.0d, 5.0d))),
      ArrayType(DoubleType, containsNull = false),
      "Array(Float64)"),
    (
      "decimalArrayColumn",
      Seq(
        Row(Array(
          new java.math.BigDecimal("1.23"),
          new java.math.BigDecimal("2.34"),
          new java.math.BigDecimal("3.45"),
          new java.math.BigDecimal("4.56"),
          new java.math.BigDecimal("5.67")))),
      ArrayType(DecimalType(9, 2), containsNull = false),
      "Array(Decimal(9, 2))"),
    (
      "dateArrayColumn",
      Seq(
        Row(
          Array(
            java.sql.Date.valueOf("2022-01-01"),
            java.sql.Date.valueOf("2022-01-02"),
            java.sql.Date.valueOf("2022-01-03")))),
      ArrayType(DateType, containsNull = false),
      "Array(Date)"),
    (
      "datetimeArrayColumn",
      Seq(
        Row(Array(
          java.sql.Timestamp.valueOf("2022-01-01 00:00:00.000000"),
          java.sql.Timestamp.valueOf("2022-01-02 11:11:11.111111"),
          java.sql.Timestamp.valueOf("2022-01-03 22:22:22.222222")))),
      ArrayType(TimestampType, containsNull = false),
      "Array(DateTime64(6))"))

  forAll(testWriteArrayCases) {
    (
        columnName: String,
        insertedData: Seq[Row],
        expectedType: DataType,
        expectedClickhouseType: String) =>
      test(s"write ClickHouse Array for $columnName column") {

        val schema = StructType(Array(StructField(columnName, expectedType, nullable = false)))
        val df = spark.createDataFrame(spark.sparkContext.parallelize(insertedData), schema)

        df.write
          .format("jdbc")
          .option("url", jdbcUrl)
          .option("dbtable", tableName)
          .option("user", jdbcUser)
          .option("password", jdbcPassword)
          .option("createTableOptions", "ENGINE = TinyLog")
          .mode("errorIfExists")
          .save()

        val actualColumnType = getColumnType(columnName)
        assert(actualColumnType == expectedClickhouseType)
      }
  }
}
