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