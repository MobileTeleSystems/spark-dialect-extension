name := "spark-dialect-extension"

organization := "ru.mts.doetl"
organizationName := "Mobile Telesystems"

version := "0.1"
// minimum compatible version with scalafix
scalaVersion := "2.12.19"

description := "Spark dialect extension for enhanced type handling."

homepage := Some(url("https://theplatform.ru/platforms/dataops"))

startYear := Some(2024)

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")


inThisBuild(List(
  semanticdbEnabled := true, // enable SemanticDB
  semanticdbVersion := scalafixSemanticdb.revision,
  scalaVersion := "2.12.19",
))
// want unused params (needed for scalafix)
scalacOptions += "-Ywarn-unused"

// core Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  // latest compatible with Java 8
  "ch.qos.logback" % "logback-classic" % "1.3.14")

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  // latest compatible with Java 8
  "io.github.cdimascio" % "dotenv-java" % "2.3.2" % "test",

  // clickhouse dependency
  "com.clickhouse" % "clickhouse-jdbc" % "0.6.0-patch5" % "test",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1" % "test",
  "org.apache.httpcomponents.core5" % "httpcore5" % "5.1.5" % "test")
