# Contributing to Spark Dialect Extension

This document provides detailed steps to build the Spark Dialect Extension from the source code.

### Prerequisites

Before you start, ensure you have the following installed:
- **Java**: Java 8 or higher. [Java Installation Guide](https://adoptopenjdk.net/)
- **Scala**: [Scala Installation Guide](https://scala-lang.org/download/)
- **SBT**: [SBT Installation Guide](https://www.scala-sbt.org/download.html)

### Compile the Project

To compile the project and generate a JAR file, run the following command in the project's root directory:

```bash
sbt package
```

This command compiles the source code and packages it into a .jar file located in the ``target/scala-2.12`` directory.


## Running Scala Tests

This section describes how to run Scala tests for the Spark Dialect Extension.

### Start Required Services

Before running the tests, you need to start the necessary database services using Docker Compose:

```bash
docker-compose -f docker-compose.test.yml up -d
```

### Execute Tests
To run the Scala tests, execute:

```bash
sbt test
```

### With Coverage Report
To run the tests with coverage and generate a report, use:

```bash
sbt clean coverage test coverageReport
```

After the tests, you can view the coverage report by opening the ``target/scala-2.12/scoverage-report/index.html`` file in your web browser.

### Stopping Docker Containers
After completing the tests, you can stop the Docker containers with:

```bash
docker-compose -f docker-compose.test.yml down
```

# Code Formatting and Linting

## Using Scalafmt to Format Code

To format all Scala source files in the project, execute the following command from the project's root directory:
```bash
sbt scalafmtAll
```

## Using Scalafix for Linting and Refactoring

To lint and refactor the code, run Scalafix using the following command:
```bash
sbt scalafixAll
```
This command checks the code against various rules specified in the ```.scalafix.conf``` file and applies fixes where possible.