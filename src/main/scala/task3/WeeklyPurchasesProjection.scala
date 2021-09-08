package org.example
package task3

import task1.TargetDataframe.generatePurchasesAttributionProjection

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql._


object WeeklyPurchasesProjection {
  // INPUT
  private val CLICKSTREAM_DATA_PATH = "capstone-dataset/mobile_app_clickstream/*.csv.gz"  // clickstream dataset in .csv.gz format
  private val PURCHASES_DATA_PATH = "capstone-dataset/user_purchases/*.csv.gz"  // purchases projection dataset in .csv.gz format
  private val CLICKSTREAM_PARQUET_DATA_PATH = "src/main/resources/task3_results/parquet_dataset/mobile_app_clickstream"  // clickstream dataset in parquet format
  private val PURCHASES_PARQUET_DATA_PATH = "src/main/resources/task3_results/parquet_dataset/user_purchases" // purchases projection dataset in parquet format
  // OUTPUT
  private val WEEKLY_PURCHASES_PROJECTION_OUTPUT = "src/main/resources/task3_results/weekly_purchases_projection"

  val log: Logger = LogManager.getRootLogger

  /*
    Select year and quarter of data
   */
  val yearOfData = 2020 // year
  val quarterOfYear = Quarter.Q4  // quarter

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Build Weekly purchases Projection within one quarter")
      .master("local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  def main(args: Array[String]): Unit = {

    // if parquet input dataset doesn't exists
    if (!checkPathExists(CLICKSTREAM_PARQUET_DATA_PATH)) {
      log.warn("No parquet datasets found - creating new one")

      writeAsParquetWithPartitioningByDate(readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema), CLICKSTREAM_PARQUET_DATA_PATH, "eventTime")
      writeAsParquetWithPartitioningByDate(readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema), PURCHASES_PARQUET_DATA_PATH, "purchaseTime")
    } else log.warn("Skipped converting .csv datasets to parquet format - datasets already exist")

    val (clickStreamDataDf, purchasesDataDf) = getDataFrom(yearOfData, quarterOfYear)

    val result = buildWeeklyPurchasesProjectionPerQuarter(
      generatePurchasesAttributionProjection(clickStreamDataDf, purchasesDataDf),
      "purchaseTime"
    )

    result.show(truncate = false)
    writeAsParquetWithPartitioning(result, WEEKLY_PURCHASES_PROJECTION_OUTPUT, "year", "quarter", "weekOfQuarter")

    spark.stop()
  }

  private def getDataFrom(yearNumber: Int, quarter: Quarter): (DataFrame, DataFrame) = {
    val months = quarter match {
      case Quarter.Q1 => "1,2,3"
      case Quarter.Q2 => "4,5,6"
      case Quarter.Q3 => "7,8,9"
      case Quarter.Q4 => "10,11,12"
      case Quarter.ALL => "*"
    }

    val clickstreamPath = s"$CLICKSTREAM_PARQUET_DATA_PATH/year=$yearNumber/month={$months}/*"
    val purchasesPath = s"$PURCHASES_PARQUET_DATA_PATH/year=$yearNumber/month={$months}/*"

    try {
      val clickStreamDataParquet = readParquet(spark, clickstreamPath, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesPath, purchasesDataSchema)

      (clickStreamDataParquet, purchasesDataParquet)
    } catch {
      case _: AnalysisException => log.warn("No input data for given time range")
      (spark.createDataFrame(spark.sparkContext.emptyRDD[Row], clickStreamDataSchema), spark.createDataFrame(spark.sparkContext.emptyRDD[Row], purchasesDataSchema))
    }
  }

  private def buildWeeklyPurchasesProjectionPerQuarter(summedDf: DataFrame, timestampCol: String) = {
    val w1 = Window.partitionBy("year","quarter").orderBy("weekOfYear")

    if (checkColumnCorrectness(summedDf, timestampCol, "timestamp")) {
      summedDf
        .withColumn("year", year(col(timestampCol)))
        .withColumn("quarter", quarter(col(timestampCol)))
        .withColumn("weekOfYear", weekofyear(col(timestampCol)))
        .withColumn("weekOfQuarter", dense_rank().over(w1))
    } else {
      throw new NoSuchFieldException(s"Given '$timestampCol' column has wrong type or doesn't exist")
    }
  }
}