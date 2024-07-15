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

