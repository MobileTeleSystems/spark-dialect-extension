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
Use Scalafmt to format your code by running:
```bash
sbt scalafmtAll
```

Use Scalafix to lint and refactor your code by running:
```bash
sbt scalafixAll
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


add Scalafix with Scalafmt in the continuous (CI) workflow to enhance code quality automatically, add an auto-commit changes to emulate the behavior of pre-commit hooks used in our python repositories. 

