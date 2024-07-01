package ru.mts.doetl.sparkdialectextensions.clickhouse

import io.github.cdimascio.dotenv.Dotenv
import org.scalatest.{BeforeAndAfterAll, Suite}
import scala.util.Random

import java.sql.DriverManager

trait ClickhouseFixture extends BeforeAndAfterAll { self: Suite =>
  val dotenv: Dotenv = Dotenv.load()

  val jdbcHostname: String = dotenv.get("CH_HOST")
  val jdbcPort: String = dotenv.get("CH_PORT")
  val jdbcPortClient: String = dotenv.get("CH_PORT_CLIENT")
  val database: String = dotenv.get("CH_DATABASE")
  val jdbcUser: String = dotenv.get("CH_USER")
  val jdbcPassword: String = dotenv.get("CH_PASSWORD")
  val tableName: String = Random.alphanumeric.take(10).mkString
  val jdbcUrl: String = s"jdbc:clickhouse://$jdbcHostname:${jdbcPort}/$database"

  val connectionProps = new java.util.Properties()

  override def beforeAll(): Unit = {
    super.beforeAll()
    val connection = DriverManager.getConnection(jdbcUrl, connectionProps)
    val statement = connection.createStatement()

    statement.executeUpdate(s"DROP TABLE IF EXISTS $tableName")
    statement.executeUpdate(
      s"""
         |CREATE TABLE $tableName (
         |  booleanColumn Boolean,
         |  byteColumn Int8,
         |  shortColumn Int16,
         |  timestampColumn Datetime64(6),
         |  jsonColumn String
         |) ENGINE = TinyLog
       """.stripMargin)

    statement.executeUpdate(
      s"""
         |INSERT INTO $tableName (booleanColumn, byteColumn, shortColumn, timestampColumn, jsonColumn)
         |VALUES
         | (true, 1, 150, now(), '{"created_by":"clickhouse_driver"}'),
         | (false, 2, 300, now(), '{"created_by":"clickhouse_driver"}')
       """.stripMargin)

    statement.close()
    connection.close()
  }

  override def afterAll(): Unit = {
    val connection = DriverManager.getConnection(jdbcUrl, connectionProps)
    val statement = connection.createStatement()

    // clean up the table after tests
    statement.executeUpdate(s"DROP TABLE IF EXISTS $tableName")

    statement.close()
    connection.close()

    super.afterAll()
  }
}
