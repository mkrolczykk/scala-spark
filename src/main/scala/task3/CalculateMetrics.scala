package org.example
package task3

import org.apache.log4j.LogManager
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import task1.TargetDataframe.generatePurchasesAttributionProjection
import task2.ChannelsStatistics.{calculateCampaignsRevenueSqlQuery, channelsEngagementPerformanceSqlQuery}

import org.apache.spark.sql.functions.{col, dayofmonth, month, year}

object CalculateMetrics {
  // INPUT
  private val CLICKSTREAM_DATA_PATH = "capstone-dataset/mobile_app_clickstream/*.csv.gz"  // clickstream dataset in .csv.gz format
  private val PURCHASES_DATA_PATH = "capstone-dataset/user_purchases/*.csv.gz"  // purchases projection dataset in .csv.gz format
  private val CLICKSTREAM_PARQUET_DATA_PATH = "src/main/resources/task3_results/parquet_dataset/mobile_app_clickstream"  // clickstream dataset in parquet format
  private val PURCHASES_PARQUET_DATA_PATH = "src/main/resources/task3_results/parquet_dataset/user_purchases" // purchases projection dataset in parquet format
  // OUTPUT
  private val QUERY_PLANS_PATH = "src/main/resources/task3_results"

  val log = LogManager.getRootLogger

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Organize data warehouse and calculate metrics for time period")
      .master("local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {

    // if parquet input dataset doesn't exists
    if (!checkPathExists(CLICKSTREAM_PARQUET_DATA_PATH)) {
      log.warn("No parquet datasets found - creating new one")

      writeAsParquetWithPartitioningByDate(readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema), CLICKSTREAM_PARQUET_DATA_PATH, "eventTime")
      writeAsParquetWithPartitioningByDate(readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema), PURCHASES_PARQUET_DATA_PATH, "purchaseTime")
    } else log.warn("Skipped converting .csv datasets to parquet format - datasets already exist")

    /**
     * task #3.1 - performance on top Csv input
     */
//    task1TopCsvInputPerformance()

    /**
     * task #3.1 - performance on parquet input
     */
//    task1ParquetInputPerformance()

    /**
     * task #3.2 - Calculate metrics from Task #2 for
     * - September 2020
     */
    // Top 10 marketing campaigns that bring the biggest revenue with csv input
//    task2CalcMetricsForYearAndMonthWithCsvInput(calculateCampaignsRevenueSqlQuery, 2020, 9, "Task #2.1 with Csv input for September 2020")

    // the most popular channel that drives the highest amount of unique sessions (engagements) with csv input
//    task2CalcMetricsForYearAndMonthWithCsvInput(channelsEngagementPerformanceSqlQuery, 2020, 9, "Task #2.2 with Csv input for September 2020")

    // Top 10 marketing campaigns that bring the biggest revenue with partitioned parquet input
//    task2CalcMetricsForYearAndMonthWithParquetInput(calculateCampaignsRevenueSqlQuery, 2020, 9, "Task #2.1 with partitioned parquet input for September 2020")

    // the most popular channel that drives the highest amount of unique sessions (engagements) with partitioned parquet input
//    task2CalcMetricsForYearAndMonthWithParquetInput(channelsEngagementPerformanceSqlQuery, 2020, 9, "Task #2.2 with partitioned parquet input for September 2020")

    /**
     * task #3.2 - Calculate metrics from Task #2 for
     * - 2020-11-11
     */
    // Top 10 marketing campaigns that bring the biggest revenue with csv input
//    task2CalcMetricsForDayWithCsvInput(calculateCampaignsRevenueSqlQuery, 2020, 11, 11, "Task #2.1 with Csv input for 2020-11-11")

    // the most popular channel that drives the highest amount of unique sessions (engagements) with csv input
//    task2CalcMetricsForDayWithCsvInput(channelsEngagementPerformanceSqlQuery, 2020, 11, 11, "Task #2.2 with Csv input for 2020-11-11")

    // Top 10 marketing campaigns that bring the biggest revenue with partitioned parquet input
//    task2CalcMetricsForDayWithParquetInput(calculateCampaignsRevenueSqlQuery, 2020, 11, 11, "Task #2.1 with partitioned parquet input for 2020-11-11")

    // the most popular channel that drives the highest amount of unique sessions (engagements) with partitioned parquet input
//    task2CalcMetricsForDayWithParquetInput(channelsEngagementPerformanceSqlQuery, 2020, 11, 11, "Task #2.2 with partitioned parquet input for 2020-11-11")

    spark.close()
  }

  def writeAsParquetWithPartitioningByDate(dfToSave: DataFrame, path: String, partitionCol: String): Unit = {

    if (checkColumnCorrectness(dfToSave, partitionCol, "timestamp")) {
      dfToSave
        .withColumn("year", year(col(partitionCol)))
        .withColumn("month", month(col(partitionCol)))
        .withColumn("day", dayofmonth(col(partitionCol)))
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy("year", "month", "day")
        .parquet(path)
    } else {
      log.error(s"Write as parquet with given partition column operation failed - Given '$partitionCol' column has wrong type or doesn't exist")
      log.warn("Saving data with partitioning by default")
      writeAsParquet(dfToSave, path)
    }
  }

  private def time[R](block: => R)(logMessage: String): R = {
    val unit = 1000000000.0 // seconds
    val unitName = "s"

    val t0: Double = System.nanoTime() / unit
    val result = block    // call-by-name
    val t1: Double = System.nanoTime() / unit
    println(logMessage + "\n\tExecution time: " + math.BigDecimal(t1 - t0).setScale(3, BigDecimal.RoundingMode.HALF_UP) + unitName)
    result
  }

  private def saveToDisk(data: String, path: String): Unit = {
    import java.io.PrintWriter
    new PrintWriter(path) { write(data); close() }
  }

  private def task1TopCsvInputPerformance(): Unit = {
    time {
      val clickStreamDataCsv = readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema)
      val purchasesDataCsv = readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema)

      val df = generatePurchasesAttributionProjection(clickStreamDataCsv, purchasesDataCsv)

      df.show(truncate = false) // force an action
    }("Task #1 with Csv input")
  }

  private def task1ParquetInputPerformance(): Unit = {
    time {
      val clickStreamDataParquet = readParquet(spark, CLICKSTREAM_PARQUET_DATA_PATH, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, PURCHASES_PARQUET_DATA_PATH, purchasesDataSchema)

      val df = generatePurchasesAttributionProjection(clickStreamDataParquet, purchasesDataParquet)

      df.show(truncate = false)
    }("Task #1 with Parquet input")
  }

  private def task2CalcMetricsForYearAndMonthWithCsvInput(f: String => String, yearNumber: Int, monthNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickStreamDataCsv = readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema)
        .filter(
          year(col("eventTime")) === yearNumber &&
          month(col("eventTime")) === monthNumber
        )

      val purchasesDataCsv = readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema)
        .filter(
          year(col("purchaseTime")) === yearNumber &&
          month(col("purchaseTime")) === monthNumber
        )

      calculate(clickStreamDataCsv, purchasesDataCsv, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$QUERY_PLANS_PATH/$logMessage.md")
  }

  private def task2CalcMetricsForYearAndMonthWithParquetInput(f: String => String, yearNumber: Int, monthNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickstreamPath = s"$CLICKSTREAM_PARQUET_DATA_PATH/year=$yearNumber/month=$monthNumber/*"
      val purchasesPath = s"$PURCHASES_PARQUET_DATA_PATH/year=$yearNumber/month=$monthNumber/*"

      val clickStreamDataParquet = readParquet(spark, clickstreamPath, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesPath, purchasesDataSchema)

      calculate(clickStreamDataParquet, purchasesDataParquet, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$QUERY_PLANS_PATH/$logMessage.md")
  }

  private def task2CalcMetricsForDayWithCsvInput(f: String => String, yearNumber: Int, monthNumber: Int, dayNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickStreamDataCsv = readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema)
        .filter(
          year(col("eventTime")) === yearNumber &&
          month(col("eventTime")) === monthNumber &&
          dayofmonth(col("eventTime")) === dayNumber
        )

      val purchasesDataCsv = readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema)
        .filter(
          year(col("purchaseTime")) === yearNumber &&
          month(col("purchaseTime")) === monthNumber &&
          dayofmonth(col("purchaseTime")) === dayNumber
        )

      calculate(clickStreamDataCsv, purchasesDataCsv, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$QUERY_PLANS_PATH/$logMessage.md")
  }

  private def task2CalcMetricsForDayWithParquetInput(f: String => String, yearNumber: Int, monthNumber: Int, dayNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickstreamPath = s"$CLICKSTREAM_PARQUET_DATA_PATH/year=$yearNumber/month=$monthNumber/day=$dayNumber"
      val purchasesPath = s"$PURCHASES_PARQUET_DATA_PATH/year=$yearNumber/month=$monthNumber/day=$dayNumber"

      val clickStreamDataParquet = readParquet(spark, clickstreamPath, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesPath, purchasesDataSchema)

      calculate(clickStreamDataParquet, purchasesDataParquet, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$QUERY_PLANS_PATH/$logMessage.md")
  }

  private def calculate(clickstreamDf: DataFrame, purchasesDf: DataFrame, f: String => String): DataFrame = {
    val summedDf = generatePurchasesAttributionProjection(clickstreamDf, purchasesDf)

    val viewName = s"summedDf"
    summedDf.createOrReplaceTempView(viewName)
    val result = spark.sql(f(viewName))

    result.show(truncate = false)
    result
  }
}
