# Spark Dialect Extension

## Overview
This repository hosts the Spark Dialect Extension, which provides custom handling for specific JDBC data types within Apache Spark. 

## Prerequisites
Before you begin, ensure you have the following prerequisites installed:
- **Java**: Java 8 or higher required. [Java Installation Guide](https://adoptopenjdk.net/)
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
docker-compose -f docker-compose.test.yml up -d
```

### Running Scala Tests:
To execute the Scala tests, use the following:
```bash
sbt test
```

##### With coverage report:
To run the tests with coverage and generate a report, use the following:
```bash
sbt clean coverage test coverageReport
```
After running the tests with coverage, you can view the coverage report by opening the following file in your web browser:
``spark-dialect-extension/target/scala-2.12/scoverage-report/index.html``

### Stopping Docker Containers:
After the tests, you can stop the Docker containers with:

``` bash
docker-compose -f docker-compose.test.yml down
```