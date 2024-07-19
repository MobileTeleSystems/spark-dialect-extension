package ru.mts.doetl.sparkdialectextensions

import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor3}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import ru.mts.doetl.sparkdialectextensions.clickhouse.{ClickhouseDataframeGenerator, ClickhouseFixture}

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

  val testCases: TableFor3[String, String, DataType] = Table(
    ("columnDefinition", "insertedData", "expectedType"),
    (
      "charArrayColumn Array(String)",
      "(['a', 'b', 'c', 'd', 'e'])",
      ArrayType(StringType, containsNull = false)),
    (
      "intArrayColumn Array(Int8)",
      "([1, 2, 3, 4, 5])",
      ArrayType(ByteType, containsNull = false)),
    (
      "shortArrayColumn Array(Int16)",
      "([1, 2, 3, 4, 5])",
      ArrayType(ShortType, containsNull = false)),
    (
      "intArrayColumn Array(Int32)",
      "([1, 2, 3, 4, 5])",
      ArrayType(IntegerType, containsNull = false)),
    (
      "longArrayColumn Array(Int64)",
      "([1, 2, 3, 4, 5])",
      ArrayType(LongType, containsNull = false)),
    (
      "floatArrayColumn Array(Float32)",
      "([1.0, 2.0, 3.0, 4.0, 5.0])",
      ArrayType(FloatType, containsNull = false)),
    (
      "doubleArrayColumn Array(Float64)",
      "([1.0, 2.0, 3.0, 4.0, 5.0])",
      ArrayType(DoubleType, containsNull = false)),
    (
      "dateArrayColumn Array(Date)",
      "(['2022-01-01', '2022-01-02', '2022-01-03'])",
      ArrayType(DateType, containsNull = false)),
    (
      "timestampArrayColumn Array(DateTime)",
      "(['2022-01-01 00:00:00', '2022-01-02 00:00:00', '2022-01-03 00:00:00'])",
      ArrayType(TimestampType, containsNull = false)),
    (
      "decimalArrayColumn Array(Decimal(9,2))",
      "([1.23, 2.34, 3.45, 4.56, 5.67])",
      ArrayType(DecimalType(9, 2), containsNull = false)))
  forAll(testCases) { (columnDefinition: String, insertedData: String, expectedType: DataType) =>
    test(s"read ClickHouse Array for ${columnDefinition} column") {
      setupTable(columnDefinition)
      insertTestData(Seq(insertedData))

      val df = spark.read
        .format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", tableName)
        .load()

      assert(df.schema.fields.head.dataType === expectedType)

      val data = df.collect().map(_.getAs[Seq[Any]](df.schema.fields.head.name)).head
      val expectedData = expectedType match {
        case ArrayType(StringType, _) =>
          insertedData.stripPrefix("(['").stripSuffix("'])").split("', '").toSeq
        case ArrayType(_, _) =>
          insertedData
            .stripPrefix("([")
            .stripSuffix("])")
            .split(", ")
            .map {
              case s if s.startsWith("'") && s.endsWith("'") =>
                s.stripPrefix("'").stripSuffix("'")
              case other => other
            }
            .toSeq
      }
      assert(data == expectedData)
    }
  }
}
