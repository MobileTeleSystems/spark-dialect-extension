package ru.mts.doetl.sparkdialectextensions

import scala.util.matching.Regex
import org.apache.spark.sql.jdbc.{JdbcDialect, JdbcType}
import org.apache.spark.sql.execution.datasources.jdbc.{JdbcUtils}
import org.apache.spark.sql.types._
import org.slf4j.LoggerFactory
import java.sql.Types

private object ClickhouseDialectExtension extends JdbcDialect {

  private val logger = LoggerFactory.getLogger(getClass)

  private val arrayTypePattern: Regex = "^Array\\((.*)\\)$".r
  private val nullableTypePattern: Regex = "^Nullable\\((.*)\\)$".r
  private val dateTypePattern: Regex = "^[dD][aA][tT][eE]$".r
  private val dateTimeTypePattern: Regex =
    "^[dD][aA][tT][eE][tT][iI][mM][eE](64)?(\\((.*)\\))?$".r
  private val decimalTypePattern: Regex =
    "^[dD][eE][cC][iI][mM][aA][lL]\\((\\d+),\\s*(\\d+)\\)$".r
  private val decimalTypePattern2: Regex =
    "^[dD][eE][cC][iI][mM][aA][lL](32|64|128|256)\\((\\d+)\\)$".r
  private val enumTypePattern: Regex = "^Enum(8|16)$".r
  private val fixedStringTypePattern: Regex = "^FixedString\\((\\d+)\\)$".r

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
    val scale = md.build.getLong("scale").toInt
    sqlType match {
      case Types.ARRAY =>
        unwrapNullable(typeName) match {
          case (_, arrayTypePattern(nestType)) =>
            toCatalystType(Types.ARRAY, nestType, size, scale, md).map {
              case (nullable, dataType) => ArrayType(dataType, nullable)
            }
          case _ => None
        }
      case _ => toCatalystType(sqlType, typeName, size, scale, md).map(_._2)
    }
  }

  private def toCatalystType(
      sqlType: Int,
      typeName: String,
      precision: Int,
      scale: Int,
      md: MetadataBuilder): Option[(Boolean, DataType)] = {
    val (nullable, _typeName) = unwrapNullable(typeName)
    val dataType = _typeName match {
      case "String" | "UUID" | fixedStringTypePattern() | enumTypePattern(_) =>
        logger.debug(s"Custom mapping applied: StringType for '${_typeName}'")
        Some(StringType)
      case "Int8" =>
        logger.debug(s"Custom mapping applied: ByteType for 'Int8'")
        Some(ByteType)
      case "UInt8" | "Int16" =>
        logger.debug(s"Custom mapping applied: ShortType for '${_typeName}'")
        Some(ShortType)
      case "UInt16" | "Int32" =>
        logger.debug(s"Custom mapping applied: IntegerType for '${_typeName}'")
        Some(IntegerType)
      case "UInt32" | "Int64" | "UInt64" | "IPv4" =>
        logger.debug(s"Custom mapping applied: LongType for '${_typeName}'")
        Some(LongType)
      case "Int128" | "Int256" | "UInt256" =>
        logger.debug(s"Type '${_typeName}' is not supported")
        None
      case "Float32" =>
        logger.debug(s"Custom mapping applied: FloatType for 'Float32'")
        Some(FloatType)
      case "Float64" =>
        logger.debug(s"Custom mapping applied: DoubleType for 'Float64'")
        Some(DoubleType)
      case dateTypePattern() =>
        logger.debug(s"Custom mapping applied: DateType for '${_typeName}'")
        Some(DateType)
      case dateTimeTypePattern() =>
        logger.debug(s"Custom mapping applied: TimestampType for '${_typeName}'")
        Some(TimestampType)
      case decimalTypePattern(precision, scale) =>
        logger.debug(
          s"Custom mapping applied: DecimalType($precision, $scale) for '${_typeName}'")
        Some(DecimalType(precision.toInt, scale.toInt))
      case decimalTypePattern2(w, scale) =>
        w match {
          case "32" =>
            logger.debug(s"Custom mapping applied: DecimalType(9, $scale) for 'Decimal$w'")
            Some(DecimalType(9, scale.toInt))
          case "64" =>
            logger.debug(s"Custom mapping applied: DecimalType(18, $scale) for 'Decimal$w'")
            Some(DecimalType(18, scale.toInt))
          case "128" =>
            logger.debug(s"Custom mapping applied: DecimalType(38, $scale) for 'Decimal$w'")
            Some(DecimalType(38, scale.toInt))
          case "256" =>
            logger.debug(s"Custom mapping applied: DecimalType(76, $scale) for 'Decimal$w'")
            Some(
              DecimalType(76, scale.toInt)
            ) // throw exception, spark support precision up to 38
        }
      case _ =>
        logger.debug(
          s"No custom mapping for typeName: ${_typeName}, default driver mapping is used")
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
      Some(JdbcType("Int16", Types.SMALLINT))
    case TimestampType =>
      logger.debug("Custom mapping applied: Datetime64(6) for 'TimestampType'")
      Some(JdbcType("Datetime64(6)", Types.TIMESTAMP))
    case ArrayType(et, _) =>
      logger.debug("Custom mapping applied: Array[T_1] for ArrayType(T_0)")
      getJDBCType(et)
        .orElse(JdbcUtils.getCommonJDBCType(et))
        .map(jdbcType => JdbcType(s"Array(${jdbcType.databaseTypeDefinition})", Types.ARRAY))
    case _ =>
      logger.debug(
        s"No custom JDBC type mapping for DataType: ${dt.simpleString}, default driver mapping is used")
      None
  }
}
