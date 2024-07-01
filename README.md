# Spark Dialect Extension

## Overview
This repository hosts the Spark Dialect Extension, which provides custom handling for specific JDBC data types within Apache Spark. 

## Prerequisites
Before you begin, ensure you have the following prerequisites installed:
- **Java**: Java 11 or higher is required for compilation step. This is necessary because certain libraries such as logging frameworks, are compiled with Java 11. This ensures compatibility and stability of the runtime environment. [Java Installation Guide](https://adoptopenjdk.net/)
- **Scala**: [Scala Installation Guide](https://scala-lang.org/download/)
- **SBT**: [SBT Installation Guide](https://www.scala-sbt.org/download.html)

## Getting Started
### Clone the Repository:
```bash
git clone https://github.com/MobileTeleSystems/spark-dialect-extension.git
cd spark-dialect-extension
```

### Format Source Code:
Run the following command to format the Scala source files using Scalafmt:
```bash
sbt scalafmtSbt
```

### Build the Project:
Compile the project and generate a JAR file:
```bash
sbt package
```
This will place the generated `.jar` file in the `target/scala-2.12` directory.


### Testing Setup
Before running the tests, start the necessary database services using Docker Compose:

``` bash
docker-compose up -d
```

### Running Scala Tests:
To execute the Scala tests, use the following command:
```bash
sbt test
```

### Stopping Docker Containers:
After the tests, you can stop the Docker containers with:

``` bash
docker-compose down
```