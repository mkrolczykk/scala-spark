package org.example
package task3

import task1.TargetDataframe.generatePurchasesAttributionProjection
import task1.{COL_EVENT_TIME, COL_PURCHASE_TIME}

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.TimestampType
import com.typesafe.config.ConfigFactory
import org.example.{log => log}

import java.nio.file.Paths

object WeeklyPurchasesProjection extends Task {
  override val taskId: String = "task3"

  override def checkTaskInput(args: Array[String]): Unit = {
    if(!(args.length >= 3)) {
      log.error("Not enough arguments, enter <config-file-path> <year> <quarter> values")
      System.exit(0)
    }
  }

  def createWeeklyPurchasesProjection(args: Array[String]): Unit = {
    val log: Logger = LogManager.getRootLogger

    checkTaskInput(args)

    val configFile = Paths.get(args(0)).toFile
    val config = ConfigFactory.parseFile(configFile).getConfig(s"$appConfig.$taskId")
    val sparkConfig: Map[String, String] = getSparkConfig(config)

    val CLICKSTREAM_DATA_PATH = config.getString("clickstream-csv-input")
    val PURCHASES_DATA_PATH = config.getString("purchases-csv-input")
    val CLICKSTREAM_PARQUET_DATA_PATH = config.getString("clickstream-parquet-input")
    val PURCHASES_PARQUET_DATA_PATH = config.getString("purchases-parquet-input")
    val WEEKLY_PURCHASES_PROJECTION_OUTPUT = config.getString("weekly-purchases-output")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Build Weekly purchases Projection within one quarter")
        .master(config.getString("master"))
        .getOrCreate()
    spark.sparkContext.setLogLevel(config.getString("log-level"))
    sparkConfig.foreach {item => spark.conf.set(item._1, item._2)}

    val intRegex = """(\d+)""".r
    val yearOfData: Int = args(1) match {
      case intRegex(str) => str.toInt
      case _ => 0
    }
    val quarterOfYear = args(2) match {
      case "1" => Quarter.Q1
      case "2" => Quarter.Q2
      case "3" => Quarter.Q3
      case "4" => Quarter.Q4
      case "all" => Quarter.ALL
      case _ => Quarter.ALL
    }

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