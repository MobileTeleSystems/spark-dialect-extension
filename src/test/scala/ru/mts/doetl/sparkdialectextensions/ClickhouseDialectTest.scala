package ru.mts.doetl.sparkdialectextensions

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.apache.spark.sql.jdbc.JdbcDialects


import ru.mts.doetl.sparkdialectextensions.clickhouse.{ClickhouseDataframeGenerator, ClickhouseFixture}

class ClickhouseDialectTest extends AnyFunSuite with Matchers with SharedSparkSession with ClickhouseFixture {

  test("custom clickhouse dialect usage") {
    val dialect = JdbcDialects.get("jdbc:clickhouse")
    assert(dialect.getClass.getName.contains("ClickhouseDialectExtension"))
  }

  test("test clickHouse table creation") {
    val df = spark.read
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .load()

    assert(df.count() > 0)
  }

  test("test clickhouse creation and table insertion to existing table") {
    val generator = new ClickhouseDataframeGenerator(spark)
    val df = generator.createDataFrame()
    assert(df.count() > 0)

    df.write
      .format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", tableName)
      .option("user", jdbcUser)
      .option("password", jdbcPassword)
      .mode("append")
      .save()
  }
}
