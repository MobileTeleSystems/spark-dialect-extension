# Spark Dialect Extension

## Overview
This repository hosts the Spark Dialect Extension, which provides custom handling for specific JDBC data types within Apache Spark. 

## Prerequisites
Before you begin, ensure you have the following prerequisites installed:
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
