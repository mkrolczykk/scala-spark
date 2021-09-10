package org.example
package task3

import task1.TargetDataframe.generatePurchasesAttributionProjection
import task1.{COL_EVENT_TIME, COL_PURCHASE_TIME}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType


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

      writeAsParquetWithPartitioningByDate(readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema), CLICKSTREAM_PARQUET_DATA_PATH, COL_EVENT_TIME)
      writeAsParquetWithPartitioningByDate(readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema), PURCHASES_PARQUET_DATA_PATH, COL_PURCHASE_TIME)
    } else log.warn("Skipped converting .csv datasets to parquet format - datasets already exist")

    val clickstreamPath = parsePath(CLICKSTREAM_PARQUET_DATA_PATH, yearOfData, quarterOfYear)
    val purchasesPath = parsePath(PURCHASES_PARQUET_DATA_PATH, yearOfData, quarterOfYear)

    val (clickStreamDataDf: DataFrame, purchasesDataDf: DataFrame) = try {
      (readParquet(spark, clickstreamPath, clickStreamDataSchema), readParquet(spark, purchasesPath, purchasesDataSchema))
    } catch {
      case _: AnalysisException => log.warn("No input data for given time range - ending spark job")
      spark.stop()
      System.exit(0)
    }

    val result = buildWeeklyPurchasesProjectionPerQuarter(
      generatePurchasesAttributionProjection(clickStreamDataDf, purchasesDataDf),
      COL_PURCHASE_TIME
    )

    result.show(truncate = false)
    writeAsParquetWithPartitioning(result, WEEKLY_PURCHASES_PROJECTION_OUTPUT, COL_YEAR, COL_QUARTER, COL_WEEK_OF_YEAR)

    spark.stop()
  }

  def parsePath(path: String, yearNumber: Int, quarter: Quarter): String = {
    val months = quarter match {
      case Quarter.Q1 => "1,2,3"
      case Quarter.Q2 => "4,5,6"
      case Quarter.Q3 => "7,8,9"
      case Quarter.Q4 => "10,11,12"
      case Quarter.ALL => "*"
    }

    s"$path/year=$yearNumber/month={$months}/*"
  }

  def buildWeeklyPurchasesProjectionPerQuarter(summedDf: DataFrame, timestampCol: String): DataFrame = {
    val w1 = Window.partitionBy(COL_YEAR, COL_QUARTER).orderBy(COL_WEEK_OF_YEAR)

    if (checkDfHasColumnOfType(summedDf, timestampCol, TimestampType)) {
      summedDf
        .withColumn(COL_YEAR, year(col(timestampCol)))
        .withColumn(COL_QUARTER, quarter(col(timestampCol)))
        .withColumn(COL_WEEK_OF_YEAR, weekofyear(col(timestampCol)))
    } else {
      throw new NoSuchFieldException(s"Given '$timestampCol' column has wrong type or doesn't exist")
    }
  }
}