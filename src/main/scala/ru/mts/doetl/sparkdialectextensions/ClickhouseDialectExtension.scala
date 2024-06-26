// SPDX-FileCopyrightText: 2021-2024 MTS (Mobile Telesystems)
//SPDX-License-Identifier: Apache-2.0
package ru.mts.doetl.sparkdialectextensions

import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types.{DataType, MetadataBuilder}
import org.slf4j.LoggerFactory

private object ClickhouseDialectExtension extends JdbcDialect {

  private val logger = LoggerFactory.getLogger(getClass)

  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:clickhouse")
  }

  /**
   * Retrieve the jdbc / sql type for a given datatype. Logging the usage of the dialect extension
   * info.
   * @param dt
   *   The datatype (e.g. [[org.apache.spark.sql.types.StringType]])
   * @return
   *   The new JdbcType if there is an override for this DataType, otherwise None
   */
  override def getJDBCType(dt: DataType): Option[JdbcType] = {
    logger.info(s"Using mock dialect to get JDBC Type for DataType: ${dt.simpleString}")
    None
  }

  /**
   * A mock method to demonstrate the retrieval of the Catalyst type based on JDBC metadata.
   * @param sqlType
   *   SQL type as integer
   * @param typeName
   *   Name of the SQL type
   * @param size
   *   Size of the type (not used in mock)
   * @param md
   *   MetadataBuilder for further metadata handling (not used in mock)
   * @return
   *   Always returns None in this mock
   */
  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    logger.debug(s"Attempting to get Catalyst Type for sqlType: $sqlType, typeName: $typeName")
    None
  }
}
