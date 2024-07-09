package ru.mts.doetl.sparkdialectextensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.jdbc.JdbcDialects
import org.apache.spark.sql.types.Metadata
import org.apache.spark.sql.types.{BooleanType, ByteType, ShortType, StringType, StructField, StructType, TimestampType}

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
        |jsonColumn JSON
    """.stripMargin
    setupTable(schema, engine = "Memory")

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
    """.stripMargin  // spark does not support writing StringType() "{\"created_by\":\"spark\"}" to clickhouse JSON column
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

    assert(stripMetadata(loadedDf.schema) == stripMetadata(df.schema))
  }
}
