## Using the Spark Dialect Extension

This section provides instructions on how to configure Apache Spark to use the Spark Dialect Extension, enabling custom handling of JDBC data types.

### Using onETL with PySpark

See [onETL documentation](https://onetl.readthedocs.io) for installation instructions.

```python
from pyspark.sql import SparkSession
from onetl.connection import Clickhouse

# describe packages should be loaded by Spark
maven_packages = [
  "io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1",
  *Clickhouse.get_packages(),
]

# Create Spark session
spark = (
  SparkSession.builder
  .appName("My Spark App")
  .config("spark.jars.packages", ",".join(maven_packages))
  .getOrCreate()
)

# Register custom Clickhouse dialect
ClickhouseDialectRegistry = spark._jvm.io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry
ClickhouseDialectRegistry.register()


# use onETL to interact with Clickhouse
clickhouse = Clickhouse(
  host="my.clickhouse.hostname.or.ip",
  port=9000,
  user="someuser",
  password="******",
  spark=spark,
)

from onetl.db import DBReader, DBWriter

# onETL now can properly read some Clickhouse types
reader = DBReader(connection=clickhouse, source="mytable")
df = reader.run()

# onETL now can properly write some Clickhouse types
writer = DBWriter(connection=clickhouse, target="anothertable")
writer.run(df)
```

### Using Spark on Scala

```scala
import org.apache.spark.sql.SparkSession

// describe packages should be loaded by Spark
var maven_packages = Array(
  "io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1",
  "com.clickhouse:clickhouse-jdbc:0.6.5",
  "com.clickhouse:clickhouse-http-client:0.6.5",
  "org.apache.httpcomponents.client5:httpclient5::5.3.1",
)

val spark = SparkSession.builder()
.appName("My Spark App")
.config("spark.jars.packages", maven_packages.mkString(","))
.getOrCreate()

// Register custom Clickhouse dialect
import io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry

ClickhouseDialectRegistry.register()

// now Spark can properly handle some Clickhouse types during read & write
df = spark.read.jdbc.options(...).load()
df.write.jdbc.options(...).saveAsTable("anothertable")
```

### Using Spark Submit

Start Spark session with downloaded packages:

```bash
spark-submit --conf spark.jars.packages=io.github.mtsongithub.doetl:spark-dialect-extension_2.12:0.0.1,com.clickhouse:clickhouse-jdbc:0.6.5,com.clickhouse:clickhouse-http-client:0.6.5,org.apache.httpcomponents.client5:httpclient5::5.3.1 ...
```

And then register custom dialect in started session.

For PySpark:
```python
# Register custom Clickhouse dialect
ClickhouseDialectRegistry = spark._jvm.io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry
ClickhouseDialectRegistry.register()
```

For Scala:
```scala
// Register custom Clickhouse dialect
import io.github.mtsongithub.doetl.sparkdialectextensions.clickhouse.ClickhouseDialectRegistry

ClickhouseDialectRegistry.register()
```
