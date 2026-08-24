# Contributing Guide

Welcome! There are many ways to contribute, including submitting bug
reports, improving documentation, submitting feature requests, reviewing
new submissions, or contributing code that can be incorporated into the
project.

## Review process

For any **significant** changes please create a new GitHub issue and
enhancements that you wish to make. Describe the feature you would like
to see, why you need it, and how it will work. Discuss your ideas
transparently and get community feedback before proceeding.

Small changes can directly be crafted and submitted to the GitHub
Repository as a Pull Request. This requires creating a **repo fork** using
[instruction](https://docs.github.com/en/get-started/quickstart/fork-a-repo).

## Initial setup for local development

### Install Git

Please follow
[instruction](https://docs.github.com/en/get-started/quickstart/set-up-git).

### Clone the repo

Open terminal and run these commands:

```bash
git clone git@github.com:myuser/spark-dialect-extension.git

cd spark-dialect-extension
```

### Setup environment

Before you start, ensure you have the following installed:
- **Java**: Java 8-11 for the `spark3` profile, Java 17 for the `spark4` profile. [Java Installation Guide](https://adoptopenjdk.net/)
- **Gradle**: [Gradle Installation Guide](https://docs.gradle.org/current/userguide/installation.html)

# How to

### Build profiles

The build is cross-built against two Spark / Scala lines, selected with `-PsparkProfile`:

| Profile          | Scala | Spark  | Java  | Artifact                        |
|------------------|-------|--------|-------|---------------------------------|
| `spark3` (default) | 2.12  | 3.5.x  | 8-11  | `spark-dialect-extension_2.12`  |
| `spark4`         | 2.13  | 4.x    | 17    | `spark-dialect-extension_2.13`  |

Each profile is built with a single command; only the compatible implementation module
(`spark35` / `spark41`) is compiled. The version-agnostic registry lives in `common` and picks the
implementation at runtime from the Spark version, so one artifact per Scala version is published.

### Compile the Project

To compile the project and generate a JAR file, run (in the project's root directory):

```bash
# Scala 2.12 / Spark 3.5 (needs Java 8-11)
./gradlew jar -PsparkProfile=spark3

# Scala 2.13 / Spark 4 (needs Java 17)
./gradlew jar -PsparkProfile=spark4
```

Each command packages a single fat `.jar` (registry + dialect implementation) into the
``build/libs`` directory.

## Run Scala Tests

This section describes how to run Scala tests for the Spark Dialect Extension.

### Start Required Services

Before running the tests, you need to start the necessary database services using Docker Compose:

```bash
docker-compose -f docker-compose.test.yml up -d
```

### Execute Tests

To run the Scala tests, execute (choose the profile / driver version you want to cover):

```bash
# Spark 3.5 / Scala 2.12 (Java 8-11)
./gradlew test -PsparkProfile=spark3 -Pclickhouse.jdbc.version=0.9.8

# Spark 4 / Scala 2.13 (Java 17)
./gradlew test -PsparkProfile=spark4 -Pclickhouse.jdbc.version=0.9.8
```

The version-specific test suites live in the `spark35` / `spark41` modules; shared test
infrastructure is provided as test fixtures from the `common` module. After the tests, you can
view the coverage report by opening the ``<module>/build/reports/tests/test/index.html`` file in
your web browser.

### Stopping Docker Containers
After completing the tests, you can stop the Docker containers with:

```bash
docker-compose -f docker-compose.test.yml down
```

## Format and lint the code

### Using Scalafmt to Format Code

To format all Scala source files in the project, execute the following command from the project's root directory:
```bash
./gradlew scalafmtAll
```

### Using Scalafix for Linting and Refactoring

To lint and refactor the code, run Scalafix using the following command:
```bash
./gradlew scalafix -PsparkProfile=spark3
```
This command checks the code against various rules specified in the ```.scalafix.conf``` file and applies fixes where possible.

Scalafix is only wired into the `spark3` profile: `semanticdb-scalac` is not published for the
Scala 2.13 patch that Spark 4 pulls in, so it is disabled on `spark4`. Scalafmt (`scalafmtAll`)
runs on both profiles.

## Create a pull request

Commit your changes:

```bash
git commit -m "Commit message"
git push
```

Then open Github interface and [create pull request](https://docs.github.com/en/get-started/quickstart/contributing-to-projects#making-a-pull-request).
Please follow guide from PR body template.

After pull request is created, it get a corresponding number, e.g. 123
(`pr_number`).

## Release a new package version

Note: this is only for repo maintainers

1. Checkout to ``develop`` branch and update it to the actual state

```bash
git checkout develop
git pull -p
```

2. Copy version (it must start with **v**, e.g. **v1.0.0**)

```bash
VERSION=$(./gradlew -q printVersion)
```

3. Commit and push changes to ``develop`` branch

```bash
git add .
git commit -m "Prepare for release ${VERSION}"
git push
```

4. Merge ``develop`` branch to ``master``, **WITHOUT** squashing

```bash
git checkout master
git pull
git merge develop
git push
```

5. Add git tag to the latest commit in ``master`` branch

```bash
git tag "$VERSION"
git push origin "$VERSION"
```

6. Update version in ``develop`` branch **after release**:

```bash
git checkout develop
NEXT_VERSION=$(echo "$VERSION" | awk -F. '/[0-9]+\./{$NF++;print}' OFS=.)
sed -i "s/version = \".*\"/version = \"$NEXT_VERSION\"/" build.gradle
git add .
git commit -m "Bump version"
git push
```
