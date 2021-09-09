name := "scala_spark_capstoneProject"

version := "1.0-SNAPSHOT"

scalaVersion := "2.12.10"

idePackagePrefix := Some("org.example")

val sparkVersion = "3.1.1"
val scalatestVersion = "3.2.2"
val mockitoscalaVersion = "1.16.23"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.scalactic" %% "scalactic" % scalatestVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % Test,
  "org.mockito" %% "mockito-scala" % mockitoscalaVersion % Test
)