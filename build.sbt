import xerial.sbt.Sonatype.sonatypeCentralHost

ThisBuild / organization := "io.github.mtsongithub.doetl"
ThisBuild / organizationName := "MTS PJSC"
ThisBuild / description := "Spark dialect extension for enhanced type handling."
ThisBuild / homepage := Some(url("https://theplatform.ru/platforms/dataops"))
ThisBuild / startYear := Some(2024)
ThisBuild / licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0"))
ThisBuild / sonatypeCredentialHost := sonatypeCentralHost

// minimum compatible version with scalafix
ThisBuild / scalaVersion := "2.12.19"

lazy val commonSettings = Seq(
  semanticdbEnabled := true,
  semanticdbVersion := scalafixSemanticdb.revision
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "spark-dialect-extension",

    // warn unused params (needed for scalafix)
    scalacOptions += "-Ywarn-unused",

    libraryDependencies ++= Seq(
      // core Spark dependencies
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.1",

      // latest compatible with Java 8
      "ch.qos.logback" % "logback-classic" % "1.3.14",

      // test dependencies
      "org.scalatest" %% "scalatest" % "3.2.19" % "test",
      "org.mockito" %% "mockito-scala" % "1.17.37" % "test",
      // latest compatible with Java 8
      "io.github.cdimascio" % "dotenv-java" % "2.3.2" % "test",

      // clickhouse dependency
      "com.clickhouse" % "clickhouse-jdbc" % "0.6.0-patch5" % "test",
      "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1" % "test",
      "org.apache.httpcomponents.core5" % "httpcore5" % "5.1.5" % "test",
    )
  )
