name := "spark-dialect-extension"

organization := "ru.mts.doetl"
organizationName := "Mobile Telesystems"

version := "0.1"
scalaVersion := "2.12.10"

description := "Spark dialect extension for enhanced type handling."

homepage := Some(url("https://theplatform.ru/platforms/dataops"))

startYear := Some(2024)

licenses += "Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")

libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.1"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.5.6"
