package ru.mts.doetl.sparkdialectextensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.{BooleanType, ByteType, IntegerType, ShortType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions._

import ru.mts.doetl.sparkdialectextensions.clickhouse.{ClickhouseDataframeGenerator, ClickhouseFixture}

class ClickhouseDialectTest extends AnyFunSuite with Matchers with SharedSparkSession with ClickhouseFixture {

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
        |jsonColumn String
    """.stripMargin
    setupTable(schema)

    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    val expectedSchema = StructType(Seq(
      StructField("booleanColumn", BooleanType, nullable = true),
      StructField("byteColumn", IntegerType, nullable = true),
      StructField("shortColumn", IntegerType, nullable = true),
      StructField("timestampColumn", TimestampType, nullable = true),
      StructField("jsonColumn", StringType, nullable = true)
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
        |jsonColumn String
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

    // TODO: remove after implementing custom mapping in ClickhouseDialectExtension
    val castedLoadedDf = loadedDf.select(
      col("booleanColumn").cast(BooleanType),
      col("byteColumn").cast(ByteType),
      col("shortColumn").cast(ShortType),
      col("timestampColumn").cast(TimestampType),
      col("jsonColumn").cast(StringType)
    )

    assert(castedLoadedDf.schema == df.schema)
  }

  test("test spark dataframe create and write to new clickhouse table") {

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

    // TODO: remove after implementing custom mapping in ClickhouseDialectExtension
    val castedLoadedDf = loadedDf.select(
      col("booleanColumn").cast(BooleanType),
      col("byteColumn").cast(ByteType),
      col("shortColumn").cast(ShortType),
      col("timestampColumn").cast(TimestampType),
      col("jsonColumn").cast(StringType)
    )

    assert(castedLoadedDf.schema == df.schema)
  }
}
