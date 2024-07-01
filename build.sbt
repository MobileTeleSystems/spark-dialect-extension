name := "spark-dialect-extension"

organization := "ru.mts.doetl"
organizationName := "Mobile Telesystems"

version := "0.1"
scalaVersion := "2.12.10"

description := "Spark dialect extension for enhanced type handling."

homepage := Some(url("https://theplatform.ru/platforms/dataops"))

startYear := Some(2024)

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

// core Spark dependencies
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1",
  "ch.qos.logback" % "logback-classic" % "1.5.6")

// test dependencies
libraryDependencies ++= Seq(
  "org.scalatest" %% "scalatest" % "3.2.19" % "test",
  "io.github.cdimascio" % "dotenv-java" % "3.0.1" % "test",

  // clickhouse dependency
  "com.clickhouse" % "clickhouse-jdbc" % "0.6.0-patch5" % "test",
  "org.apache.httpcomponents.client5" % "httpclient5" % "5.3.1" % "test",
  "org.apache.httpcomponents.core5" % "httpcore5" % "5.1.5"  % "test",
)
