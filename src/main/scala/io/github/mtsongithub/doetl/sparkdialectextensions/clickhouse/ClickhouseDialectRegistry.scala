package io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects
import scala.reflect.runtime.universe

object ClickhouseDialectRegistry {

  def register(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map(_.version)
      .getOrElse(throw new IllegalStateException("No active Spark session found"))

    val dialectClassName = sparkVersion match {
      case version if version.startsWith("3.5") =>
        "io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.spark35.ClickhouseDialectExtension"
      // TODO: in future add other versions of spark
      case _ =>
        throw new UnsupportedOperationException(s"Unsupported Spark version: $sparkVersion")
    }

    try {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = mirror.staticModule(dialectClassName)
      val obj =
        mirror.reflectModule(module).instance.asInstanceOf[org.apache.spark.sql.jdbc.JdbcDialect]

      JdbcDialects.registerDialect(obj)
    } catch {
      case e: Exception =>
        throw new RuntimeException(
          s"Failed to register Clickhouse dialect for Spark version $sparkVersion",
          e)
    }
  }
}
