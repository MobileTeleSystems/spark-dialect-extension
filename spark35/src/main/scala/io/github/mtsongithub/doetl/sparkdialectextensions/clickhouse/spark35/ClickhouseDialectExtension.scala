// SPDX-FileCopyrightText: 2024-present MTS PJSC
// SPDX-License-Identifier: Apache-2.0
package io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.spark35

import org.apache.spark.sql.execution.datasources.jdbc.{JdbcOptionsInWrite, JdbcUtils}
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.sql.{Statement, Types}
import scala.util.matching.Regex

private object ClickhouseDialectExtension extends JdbcDialect {

  private val logger = LoggerFactory.getLogger(getClass)

  private val arrayTypePattern: Regex = """(?i)^Array\((.*)\)$""".r
  private val nullableTypePattern: Regex = """(?i)^Nullable\((.*)\)$""".r
  private val dateTypePattern: Regex = """(?i)^Date$""".r
  private val dateTimeTypePattern: Regex = """(?i)^DateTime(\d+)?(?:\((\d+)\))?$""".r
  private val decimalTypePattern: Regex = """(?i)^Decimal\((\d+),\s*(\d+)\)$""".r
  private val decimalTypePattern2: Regex = """(?i)^Decimal(32|64|128|256)\((\d+)\)$""".r

  /**
   * A pattern to match ClickHouse column definitions. This pattern captures the column name, data
   * type, and whether it is nullable.
   * @example
   *   "column_name" String NOT NULL, "column_name" Int32, "column_name" Decimal(10,2) etc.
   */
  private val columnPattern: Regex =
    """"([^"]+)"\s+(.+?)(?:\s+(NOT\s+NULL))?\s*(?=(?:\s*,\s*"|$))""".r

  override def canHandle(url: String): Boolean = {
    url.startsWith("jdbc:clickhouse")
  }

  /**
   * A method to demonstrate the retrieval of the Catalyst type based on JDBC metadata.
   *
   * @param sqlType
   *   SQL type as integer
   * @param typeName
   *   Name of the SQL type
   * @param size
   *   Size of the type
   * @param md
   *   MetadataBuilder for further metadata handling
   * @return
   *   The corresponding Catalyst data type.
   */
  override def getCatalystType(
      sqlType: Int,
      typeName: String,
      size: Int,
      md: MetadataBuilder): Option[DataType] = {
    sqlType match {
      case Types.ARRAY =>
        unwrapNullable(typeName) match {
          case (_, arrayTypePattern(nestType)) =>
            toCatalystType(nestType).map { case (nullable, dataType) =>
              ArrayType(dataType, nullable)
            }
          case _ => None
        }
      case _ => toCatalystType(typeName).map(_._2)
    }
  }

  private def toCatalystType(typeName: String): Option[(Boolean, DataType)] = {
    val (nullable, _typeName) = unwrapNullable(typeName)
    val dataType = _typeName match {
      case "String" | "IPv4" | "IPv6" | "UUID" =>
        logger.debug(s"Custom mapping applied: StringType for '${_typeName}'")
        Some(StringType)
      case "Int8" =>
        logger.debug(s"Custom mapping applied: ByteType for '${_typeName}'")
        Some(ByteType)
      case "UInt8" | "Int16" =>
        logger.debug(s"Custom mapping applied: ShortType for '${_typeName}'")
        Some(ShortType)
      case "UInt16" | "Int32" =>
        logger.debug(s"Custom mapping applied: IntegerType for '${_typeName}'")
        Some(IntegerType)
      case "UInt32" | "Int64" =>
        logger.debug(s"Custom mapping applied: LongType for '${_typeName}'")
        Some(LongType)
      case "UInt64" =>
        logger.debug(s"Custom mapping applied: DecimalType for '${_typeName}'")
        Some(DecimalType(38, 0))
      case "Int128" | "Int256" | "UInt256" =>
        logger.debug(s"Type '${_typeName}' is not supported")
        None
      case "Float32" =>
        logger.debug(s"Custom mapping applied: FloatType for '${_typeName}'")
        Some(FloatType)
      case "Float64" =>
        logger.debug(s"Custom mapping applied: DoubleType for '${_typeName}'")
        Some(DoubleType)
      case dateTypePattern() =>
        logger.debug(s"Custom mapping applied: DateType for '${_typeName}'")
        Some(DateType)
      case dateTimeTypePattern(_, _) =>
        logger.debug(s"Custom mapping applied: TimestampType for '${_typeName}'")
        Some(TimestampType)
      case decimalTypePattern(precision, scale) =>
        logger.debug(
          s"Custom mapping applied: DecimalType($precision, $scale) for '${_typeName}'")
        Some(DecimalType(precision.toInt, scale.toInt))
      case decimalTypePattern2(w, scale) =>
        w match {
          case "32" =>
            logger.debug(s"Custom mapping applied: DecimalType(9, $scale) for '${_typeName}'")
            Some(DecimalType(9, scale.toInt))
          case "64" =>
            logger.debug(s"Custom mapping applied: DecimalType(18, $scale) for '${_typeName}'")
            Some(DecimalType(18, scale.toInt))
          case "128" =>
            logger.debug(s"Custom mapping applied: DecimalType(38, $scale) for '${_typeName}'")
            Some(DecimalType(38, scale.toInt))
          case "256" =>
            logger.debug(s"Custom mapping applied: DecimalType(76, $scale) for '${_typeName}'")
            Some(
              DecimalType(76, scale.toInt)
            ) // throw exception, spark support precision up to 38
        }
      case _ =>
        logger.debug(
          s"No custom mapping for typeName '${_typeName}', default driver mapping is used")
        None
    }
    dataType.map((nullable, _))
  }

  /**
   * Unwraps nullable types to determine if the type is nullable and to retrieve the base type.
   * This logic is copied from the Housepower project.
   *
   * @see
   *   https://github.com/housepower/ClickHouse-Native-JDBC
   * @param maybeNullableTypeName
   *   The type name that may include Nullable.
   * @return
   *   A tuple where the first element indicates if the type is nullable, and the second element
   *   is the base type.
   */
  private def unwrapNullable(maybeNullableTypeName: String): (Boolean, String) =
    maybeNullableTypeName match {
      case nullableTypePattern(typeName) => (true, typeName)
      case _ => (false, maybeNullableTypeName)
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
      logger.debug("Custom mapping applied: Bool for 'BooleanType'")
      Some(JdbcType("Bool", Types.BOOLEAN))
    case ShortType =>
      logger.debug("Custom mapping applied: Int16 for 'ShortType'")
      // Using literal `Int16` fails on Spark 3.x - Spark converts type names to lowercase,
      // but Clickhouse type names are case-sensitive. See https://issues.apache.org/jira/browse/SPARK-46612
      // Using SMALLINT as alias for Int16, which is case-insensitive.
      Some(JdbcType("SMALLINT", Types.SMALLINT))
    case TimestampType =>
      logger.debug("Custom mapping applied: Datetime64(6) for 'TimestampType'")
      Some(JdbcType("Datetime64(6)", Types.TIMESTAMP))
    case ArrayType(et, nullable) =>
      logger.debug("Custom mapping applied: Array[T] for ArrayType(T)")
      getJDBCType(et)
        .orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jdbcType =>
          if (nullable) {
            logger.debug(
              s"Mapping to Nullable type for Array: Nullable(${jdbcType.databaseTypeDefinition})")
            JdbcType(s"Array(Nullable(${jdbcType.databaseTypeDefinition}))", Types.ARRAY)
          } else
            JdbcType(s"Array(${jdbcType.databaseTypeDefinition})", Types.ARRAY))
    case _ =>
      logger.debug(
        s"No custom JDBC type mapping for DataType: ${dt.simpleString}, default driver mapping is used")
      None
  }

  /**
   * Custom implementation of `createTable` to handle specific ClickHouse table creation options.
   * This method ensures that the column schemas are formatted correctly for ClickHouse,
   * particularly by wrapping nullable types appropriately, as the default implementation does not
   * support `Nullable` types for column schemas in ClickHouse.
   *
   * @see
   *   ›https://github.com/apache/spark/blob/branch-3.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L919-L923
   *
   * @see
   *   https://github.com/apache/spark/blob/branch-3.5/sql/core/src/main/scala/org/apache/spark/sql/execution/datasources/jdbc/JdbcUtils.scala#L823-L824
   *
   * @param statement
   *   The SQL statement object used to execute the table creation command.
   * @param tableName
   *   The name of the table to be created in the ClickHouse database.
   * @param strSchema
   *   A string representing the schema definitions for the table's columns.
   * @param options
   *   Additional options for creating the table.
   */
  override def createTable(
      statement: Statement,
      tableName: String,
      strSchema: String,
      options: JdbcOptionsInWrite): Unit = {
    statement.executeUpdate(
      s"CREATE TABLE $tableName (${parseColumnDefinitions(strSchema)}) ${options.createTableOptions}")
  }

  /**
   * Parses column definitions from a raw string to format them for ClickHouse. This method
   * transforms a string describing columns (including names, types, and constraints) into a
   * proper SQL format, ensuring that NOT NULL constraints are applied correctly.
   *
   * @param columnDefinitions
   *   A raw string representing the column definitions, formatted as "column_name column_type
   *   [NOT NULL]".
   * @return
   *   A formatted string of column definitions ready for SQL execution.
   *
   * @example
   *   Input: "id" Integer NOT NULL, "name" String, "tags" Array(Nullable(String)) <br> Output:
   *   "id" Integer NOT NULL, "name" Nullable(String), "tags" Array(Nullable(String))
   */
  private def parseColumnDefinitions(columnDefinitions: String): String = {
    columnPattern
      .findAllMatchIn(columnDefinitions)
      .flatMap { matchResult =>
        val columnName = matchResult.group(1)
        val columnType = matchResult.group(2)
        val notNull = matchResult.group(3)

        if (arrayTypePattern.findFirstIn(columnType).isDefined || notNull != null) {
          Some(s""""$columnName" $columnType""")
        } else {
          Some(s""""$columnName" Nullable($columnType)""")
        }
      }
      .mkString(", ")
  }

  override def isCascadingTruncateTable(): Option[Boolean] = Some(false)
}
