package ru.mts.doetl.sparkdialectextensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{BooleanType, ByteType, ShortType, StructField, StructType, TimestampType}

import ru.mts.doetl.sparkdialectextensions.clickhouse.{ClickhouseDataframeGenerator, ClickhouseFixture}

class ClickhouseDialectTest extends AnyFunSuite with Matchers with SharedSparkSession with ClickhouseFixture {


  /**
   * this function is used to normalize schemas for comparison purposes
   * to handle cases such as: 'StructField("shortColumn", ShortType, true, {"scale":0})'
   * is equal to 'StructField("shortColumn", ShortType, true, {})'
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

    val expectedSchema = StructType(Seq(
      StructField("booleanColumn", BooleanType, nullable = true),
      StructField("byteColumn",  ByteType, nullable = true),
      StructField("shortColumn", ShortType, nullable = true),
      StructField("timestampColumn", TimestampType, nullable = true),
    ))

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
    val schema = StructType(Seq(
      StructField("shortColumn", ShortType, nullable = true)
    ))
    val data = Seq(
      Row(Short.MinValue),
      Row(Short.MaxValue)
    )
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
    val schema = StructType(Seq(
      StructField("timestampColumn", TimestampType, nullable = true)
    ))
    val currentTime = new java.sql.Timestamp(System.currentTimeMillis())
    val data = Seq(
      Row(currentTime)
    )
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
    val schema = StructType(Seq(
      StructField("booleanColumn", BooleanType, nullable = true)
    ))
    val data = Seq(
      Row(true),
      Row(false)
    )
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
}
