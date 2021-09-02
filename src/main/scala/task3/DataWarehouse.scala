package org.example
package task3

import org.apache.spark.sql.SparkSession

object DataWarehouse {
  // INPUT
//  private val CLICKSTREAM_DATA_PATH = "capstone-dataset/mobile_app_clickstream/*.csv.gz"
//  private val PURCHASES_DATA_PATH = "capstone-dataset/user_purchases/*.csv.gz"
  private val CLICKSTREAM_DATA_PATH = "src/main/resources/mobile-app-clickstream_sample.tsv"
  private val PURCHASES_DATA_PATH = "src/main/resources/purchases_sample.tsv"
  // OUTPUT
  private val WRITE_OUTPUT_PATH = "src/main/resources/task3_results/"

  private val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Organize data warehouse and calculate metrics for time period")
      .master("local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    println("dziala")
  }
}
