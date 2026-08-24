// SPDX-FileCopyrightText: 2024-present MTS PJSC
// SPDX-License-Identifier: Apache-2.0
package io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects
import scala.reflect.runtime.universe

object ClickhouseDialectRegistry {

  /**
   * Resolves the fully-qualified name of the dialect implementation to use for a given Spark
   * version. Kept separate from [[register]] so the version dispatch can be unit-tested without
   * an active Spark session (Spark 4's `getActiveSession` filters out non-usable sessions, so
   * mocking it is not viable).
   *
   * @throws UnsupportedOperationException
   *   if no implementation is bundled for the given Spark version.
   */
  private[clickhouse] def dialectClassName(sparkVersion: String): String = sparkVersion match {
    case version if version.startsWith("3.5") =>
      "io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.spark35.ClickhouseDialectExtension"
    case version if version.startsWith("4.") =>
      // A single Scala 2.13 artifact serves every Spark 4.x minor version.
      "io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.spark41.ClickhouseDialectExtension"
    // TODO: in future add other versions of spark
    case _ =>
      throw new UnsupportedOperationException(s"Unsupported Spark version: $sparkVersion")
  }

  def register(): Unit = {
    val sparkVersion = SparkSession.getActiveSession
      .map(_.version)
      .getOrElse(throw new IllegalStateException("No active Spark session found"))

    val className = dialectClassName(sparkVersion)

    try {
      val mirror = universe.runtimeMirror(getClass.getClassLoader)
      val module = mirror.staticModule(className)
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
