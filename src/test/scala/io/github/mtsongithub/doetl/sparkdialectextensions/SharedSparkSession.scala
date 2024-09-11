package io.github.mtsongithub.doetl.sparkdialectextensions

import org.apache.spark.sql.SparkSession
import org.scalatest.BeforeAndAfterAll
import org.scalatest.Suite
import io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry

import java.nio.file.Paths

trait SharedSparkSession extends BeforeAndAfterAll { self: Suite =>
  @transient private var _spark: SparkSession = _

  def spark: SparkSession = _spark

  override def beforeAll(): Unit = {
    super.beforeAll()

    val scalaVersion: String = scala.util.Properties.versionNumberString
    val majorScalaVersion: String = scalaVersion.split('.').take(2).mkString(".")
    val jarFiles =
      Paths.get("target", s"scala-$majorScalaVersion").toFile.listFiles().filter(_.isFile)
    val jarPaths = jarFiles.map(_.getAbsolutePath).mkString(",")

    _spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark Test Session")
      .config("spark.ui.enabled", "false") // disable UI to reduce overhead
      .config("spark.jars", jarPaths) // include the JAR file containing the custom dialect
      .config("spark.driver.bindAddress", "127.0.0.1")
      .getOrCreate()

    // register custom Clickhouse dialect
    ClickhouseDialectRegistry.register()

    afterSessionCreated()
  }

  override def afterAll(): Unit = {
    try {
      if (_spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      super.afterAll()
    }
  }

  /** hook for initializing session state or running code after SparkSession is created */
  def afterSessionCreated(): Unit = {}
}
