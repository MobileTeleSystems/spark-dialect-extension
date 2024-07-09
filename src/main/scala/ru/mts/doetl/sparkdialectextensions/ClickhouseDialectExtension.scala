// SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
//SPDX-License-Identifier: Apache-2.0
package ru.mts.doetl.sparkdialectextensions

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.sql.Types

private object ClickhouseDialectExtension extends JdbcDialect {

  private val logger = LoggerFactory.getLogger(getClass)

  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:clickhouse")
  }

  /**
   * A mock method to demonstrate the retrieval of the Catalyst type based on JDBC metadata.
   *
   * @param sqlType
   * SQL type as integer
   * @param typeName
   * Name of the SQL type
   * @param size
   * Size of the type (not used in mock)
   * @param md
   * MetadataBuilder for further metadata handling (not used in mock)
   * @return
   * Always returns None in this mock
   */
  override def getCatalystType(
                                sqlType: Int,
                                typeName: String,
                                size: Int,
                                md: MetadataBuilder): Option[DataType] = (sqlType, typeName) match {
    case (Types.VARCHAR, "JSON") =>
      logger.info("Custom mapping applied: StringType for 'JSON'")
      Some(StringType)
    case (Types.TINYINT, "Int8") =>
      logger.info("Custom mapping applied: ByteType for 'Int8'")
      Some(ByteType)
    case (Types.SMALLINT, "Int16") =>
      logger.info("Custom mapping applied: ShortType for 'Int16'")
      Some(ShortType)
    case _ =>
      logger.info(s"No custom JDBC type mapping for sqlType: $sqlType, typeName: $typeName, default driver mapping is used")
      None
  }

  /**
   * Retrieve the jdbc / sql type for a given datatype. Logging the usage of the dialect extension
   * info.
   * @param dt
   *   The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return
   *   The new JdbcType if there is an override for this DataType, otherwise None
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = dt match {
    case BooleanType =>
      logger.info("Custom mapping applied: Bool for 'BooleanType'")
      Some(JdbcType("Bool", Types.BOOLEAN))
    case ShortType =>
      logger.info("Custom mapping applied: Int16 for 'ShortType'")
      Some(JdbcType("Int16", Types.SMALLINT))
    case TimestampType =>
      logger.info("Custom mapping applied: Datetime64(6) for 'TimestampType'")
      Some(JdbcType("Datetime64(6)", Types.TIMESTAMP))
    case _ =>
      logger.info(s"No custom JDBC type mapping for DataType: ${dt.simpleString}, default driver mapping is used")
      None
  }
}
