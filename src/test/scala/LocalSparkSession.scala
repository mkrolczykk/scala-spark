package org.example

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait LocalSparkSession {
  Logger.getLogger("org").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.ERROR)

  implicit lazy val spark: SparkSession = SparkSession.builder().master("local[*]").appName("Local test").getOrCreate()
}
