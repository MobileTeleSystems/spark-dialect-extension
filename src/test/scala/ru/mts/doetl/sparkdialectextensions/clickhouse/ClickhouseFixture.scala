package ru.mts.doetl.sparkdialectextensions.clickhouse

import io.github.cdimascio.dotenv.Dotenv
import org.scalatest.{BeforeAndAfterEach, Suite}
import scala.util.Random

import java.sql.{Connection, DriverManager}

trait ClickhouseFixture extends BeforeAndAfterEach { self: Suite =>
  val dotenv: Dotenv = Dotenv.load()

  val jdbcHostname: String = dotenv.get("CH_HOST")
  val jdbcPort: String = dotenv.get("CH_PORT")
  val jdbcPortClient: String = dotenv.get("CH_PORT_CLIENT")
  val database: String = dotenv.get("CH_DATABASE")
  val jdbcUser: String = dotenv.get("CH_USER")
  val jdbcPassword: String = dotenv.get("CH_PASSWORD")
  var tableName: String = _
  val jdbcUrl: String = s"jdbc:clickhouse://$jdbcHostname:${jdbcPort}/$database"

  val connectionProps = new java.util.Properties()
  var connection: Connection = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    tableName = Random.alphanumeric.take(10).mkString
    connection = DriverManager.getConnection(jdbcUrl, connectionProps)
  }

  def setupTable(tableSchema: String, engine: String = "TinyLog"): Unit = {
    val statement = connection.createStatement()
    statement.executeUpdate(s"DROP TABLE IF EXISTS $tableName")
    statement.executeUpdate(s"CREATE TABLE $tableName ($tableSchema) ENGINE = $engine")
    statement.close()
  }

  def insertTestData(data: Seq[String]): Unit = {
    val statement = connection.createStatement()
    val values = data.mkString(", ")
    statement.executeUpdate(s"INSERT INTO $tableName VALUES $values")
    statement.close()
  }

  override def afterEach(): Unit = {
    val statement = connection.createStatement()
    statement.executeUpdate(s"DROP TABLE IF EXISTS $tableName")
    statement.close()
    connection.close()
    super.afterEach()
  }
}
