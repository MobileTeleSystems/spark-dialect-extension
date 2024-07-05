package ru.mts.doetl.sparkdialectextensions.clickhouse

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

class ClickhouseDataframeGenerator(spark: SparkSession) {
  def createDataFrame(): DataFrame = {
    val data = Seq(
      (true, 1.toByte, 150.toShort, new java.sql.Timestamp(System.currentTimeMillis()), "{\"created_by\":\"spark\"}"),
      (false, 2.toByte, 300.toShort, new java.sql.Timestamp(System.currentTimeMillis()), "{\"created_by\":\"spark\"}")
    )

    val schema = StructType(
      List(
        StructField("booleanColumn", BooleanType, nullable = true),
        StructField("byteColumn", ByteType, nullable = true),
        StructField("shortColumn", ShortType, nullable = true),
        StructField("timestampColumn", TimestampType, nullable = true),
        StructField("jsonColumn", StringType, nullable = true)  // storing JSON data as StringType
      )
    )

    val rowRDD = spark.sparkContext.parallelize(data).map {
      case (boolean, byte, short, timestamp, json) => Row(boolean, byte, short, timestamp, json)
    }
    val initialDf = spark.createDataFrame(rowRDD, schema)

    initialDf
  }
}
