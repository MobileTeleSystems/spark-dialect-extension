## Using the Spark Dialect Extension

This section provides instructions on how to configure Apache Spark to use the Spark Dialect Extension, enabling custom handling of JDBC data types.

### Configuration Steps

To integrate the Spark Dialect Extension into your Spark application, you need to add the compiled JAR file to the Spark classpath. The extension, via ``<DBMS>DialectRegistry`` classes, will dynamically detect the Spark version and load the corresponding dialect.

#### Add the JAR to Spark

1. **Locate the Compiled JAR**: Ensure you have built the project and locate the `.jar`: `/path/to/spark-dialect-extension_2.12-0.1.jar` directory.

2. **Configure Spark**: Add the JAR to your Spark job's classpath by modifying the `spark.jars` configuration parameter. This can be done in several ways depending on how you are running your Spark application:

- **Spark Submit Command**:
  ```bash
  spark-submit --jars /path/to/spark-dialect-extension_2.12-0.1.jar --class YourMainClass your-application.jar
  ```

- **Programmatically** (within your Spark application):
  ```scala
  import io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry
  import org.apache.spark.sql.SparkSession
  
  val spark = SparkSession.builder()
  .appName("My Spark App")
  .config("spark.jars", "/path/to/spark-dialect-extension_2.12-0.1.jar")
  .getOrCreate()
  
  // Register custom Clickhouse dialect based on Spark version
  ClickhouseDialectRegistry.register()
  ```
