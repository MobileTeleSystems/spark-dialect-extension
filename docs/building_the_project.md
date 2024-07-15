## Building the Spark Dialect Extension

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