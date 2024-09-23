## Using the Spark Dialect Extension

This section provides instructions on how to configure Apache Spark to use the Spark Dialect Extension, enabling custom handling of JDBC data types.

### Add the JAR to Spark

#### Using release version

##### Using SparkConf

For PySpark:

```python
from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
  .appName("My Spark App")
  .config("spark.jars.packages", "io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1")
  .getOrCreate()
)
```

For Spark on Scala:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
.appName("My Spark App")
.config("spark.jars.packages", "io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1")
.getOrCreate()
```

##### Using Spark Submit

```bash
spark-submit --conf spark.jars.packages=io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1
```

#### Compile from source

##### Build .jar file

See [CONTRIBUTING.md](../CONTRIBUTING.md) for build instructions.

After build you'll have a file `/path/to/cloned-repo/target/scala_2.12/spark-dialect-extension_2.12-0.0.1.jar`

##### Using SparkConf

For PySpark:

```python
from pyspark.sql import SparkSession

spark = (
  SparkSession.builder
  .appName("My Spark App")
  .config("spark.jars", "/path/to/cloned-repo/target/scala_2.12/spark-dialect-extension_2.12-0.0.1.jar")
  .getOrCreate()
)
```

For Spark on Scala:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
.appName("My Spark App")
.config("spark.jars", "/path/to/cloned-repo/target/scala_2.12/spark-dialect-extension_2.12-0.0.1.jar")
.getOrCreate()
```

##### Using Spark Submit

```bash
spark-submit --jars /path/to/cloned-repo/target/scala_2.12/spark-dialect-extension_2.12-0.0.1.jar
```

### Register a dialect

To integrate the Spark Dialect Extension into your Spark application, you need to use ``<DBMS>DialectRegistry`` classes, which dynamically detect the Spark version and register the corresponding dialect.

For PySpark:

```python
# Register custom Clickhouse dialect
ClickhouseDialectRegistry = spark._jvm.io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry
ClickhouseDialectRegistry.register()
```

For Spark on Scala:
```scala
// Register custom Clickhouse dialect
import io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry

ClickhouseDialectRegistry.register()
```
