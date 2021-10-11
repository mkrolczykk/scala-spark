package org.example
package task3

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import task1.TargetDataframe.{generatePurchasesAttributionProjection, generatePurchasesAttributionProjectionWithUDAF}
import task2.ChannelsStatistics.{calculateCampaignsRevenueSqlQuery, channelsEngagementPerformanceSqlQuery}
import task1._

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}

import java.nio.file.Paths

object CalculateMetrics extends Task {
  override val taskId: String = "task3"

  override def checkTaskInput(args: Array[String]): Unit = {
    if(!(args.length >= 2)) {
      log.error("Not enough arguments, enter <config-file-path> <metric-type> values")
      System.exit(0)
    }
  }

  /** Main function */
  def calculateMetric(args: Array[String]): Unit = {
    val log: Logger = LogManager.getRootLogger

    checkTaskInput(args)

    val configFile = Paths.get(args(0)).toFile
    val metric = args(1)
    val config = ConfigFactory.parseFile(configFile).getConfig(s"$appConfig.$taskId")
    val sparkConfig: Map[String, String] = getSparkConfig(config)

    val CLICKSTREAM_DATA_PATH = config.getString("clickstream-csv-input")
    val PURCHASES_DATA_PATH = config.getString("purchases-csv-input")
    val CLICKSTREAM_PARQUET_DATA_PATH = config.getString("clickstream-parquet-input")
    val PURCHASES_PARQUET_DATA_PATH = config.getString("purchases-parquet-input")
    val QUERY_PLANS_PATH = config.getString("query-plans-output")

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Organize data warehouse and calculate metrics for time period")
        .master(config.getString("master"))
        .getOrCreate()
    spark.sparkContext.setLogLevel(config.getString("log-level"))
    sparkConfig.foreach {item => spark.conf.set(item._1, item._2)}

    // if parquet input dataset doesn't exists
    if (!checkPathExists(CLICKSTREAM_PARQUET_DATA_PATH)) {
      log.warn("No parquet datasets found - creating new one")

      writeAsParquetWithPartitioningByDate(readCsv(spark, CLICKSTREAM_DATA_PATH, clickStreamDataSchema), CLICKSTREAM_PARQUET_DATA_PATH, COL_EVENT_TIME)
      writeAsParquetWithPartitioningByDate(readCsv(spark, PURCHASES_DATA_PATH, purchasesDataSchema), PURCHASES_PARQUET_DATA_PATH, COL_PURCHASE_TIME)
    } else log.warn("Skipped converting .csv datasets to parquet format - datasets already exist")

    metric match {
      case "t1-csv" => task1TopCsvInputPerformance(spark, CLICKSTREAM_DATA_PATH, PURCHASES_DATA_PATH)   // Task 1 with csv input
      case "t1-csv-udaf" => task1UDAFTopCsvInputPerformance(spark, CLICKSTREAM_DATA_PATH, PURCHASES_DATA_PATH)  // Task 1 with csv input and udaf functions
      case "t1-parquet" => task1ParquetInputPerformance(spark, CLICKSTREAM_PARQUET_DATA_PATH, PURCHASES_PARQUET_DATA_PATH)   // Task 1 with parquet input
      case "t1-parquet-udaf" => task1UDAFParquetInputPerformance(spark, CLICKSTREAM_PARQUET_DATA_PATH, PURCHASES_PARQUET_DATA_PATH)  // Task 1 with parquet input and udaf functions
      case "t2-csv-revenue" => {
        task2CalcMetricsWithCsvInput(   // Task 2 revenue with csv input
          spark,
          CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          calculateCampaignsRevenueSqlQuery,
          "Task #2.1 with Csv input"
        )
      }
      case "t2-csv-engagement" => {  // Task 2 engagement with csv input
        task2CalcMetricsWithCsvInput(
          spark, CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          channelsEngagementPerformanceSqlQuery,
          "Task #2.2 with Csv input"
        )
      }
      case "t2-parquet-revenue" => {   // Task 2 revenue with parquet input
        task2CalcMetricsWithParquetInput(
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          calculateCampaignsRevenueSqlQuery,
          "Task #2.1 with parquet input"
        )
      }
      case "t2-parquet-engagement" => {   // Task 2 engagement with parquet input
        task2CalcMetricsWithParquetInput(
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          channelsEngagementPerformanceSqlQuery,
          "Task #2.2 with parquet input"
        )
      }
      case "t3-csv-revenue-sept" => {
        task2CalcMetricsForYearAndMonthWithCsvInput(  // Top 10 marketing campaigns that bring the biggest revenue with csv input for September
          spark,
          CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          QUERY_PLANS_PATH,
          calculateCampaignsRevenueSqlQuery,
          2020,
          9,
          "Task #2.1 with Csv input for September 2020"
        )
      }
      case "t3-csv-engagement-sept" => {
        task2CalcMetricsForYearAndMonthWithCsvInput(  // the most popular channel that drives the highest amount of unique sessions (engagements) with csv input for September
          spark,
          CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          QUERY_PLANS_PATH,
          channelsEngagementPerformanceSqlQuery,
          2020,
          9,
          "Task #2.2 with Csv input for September 2020"
        )
      }
      case "t3-parquet-revenue-sept" => {
        task2CalcMetricsForYearAndMonthWithParquetInput(  // Top 10 marketing campaigns that bring the biggest revenue with partitioned parquet input for September
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          QUERY_PLANS_PATH,
          calculateCampaignsRevenueSqlQuery,
          2020,
          9,
          "Task #2.1 with partitioned parquet input for September 2020"
        )
      }
      case "t3-parquet-engagement-sept" => {
        task2CalcMetricsForYearAndMonthWithParquetInput(  // the most popular channel that drives the highest amount of unique sessions (engagements) with partitioned parquet input for September
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          QUERY_PLANS_PATH,
          channelsEngagementPerformanceSqlQuery,
          2020,
          9,
          "Task #2.2 with partitioned parquet input for September 2020"
        )
      }
      case "t3-csv-revenue-day" => {
        task2CalcMetricsForDayWithCsvInput(   // Top 10 marketing campaigns that bring the biggest revenue with csv input for 2020-11-11
          spark,
          CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          QUERY_PLANS_PATH,
          calculateCampaignsRevenueSqlQuery,
          2020,
          11,
          11,
          "Task #2.1 with Csv input for 2020-11-11"
        )
      }
      case "t3-csv-engagement-day" => {
        task2CalcMetricsForDayWithCsvInput(   // the most popular channel that drives the highest amount of unique sessions (engagements) with csv input for 2020-11-11
          spark,
          CLICKSTREAM_DATA_PATH,
          PURCHASES_DATA_PATH,
          QUERY_PLANS_PATH,
          channelsEngagementPerformanceSqlQuery,
          2020,
          11,
          11,
          "Task #2.2 with Csv input for 2020-11-11")
      }
      case "t3-parquet-revenue-day" => {
        task2CalcMetricsForDayWithParquetInput(   // Top 10 marketing campaigns that bring the biggest revenue with partitioned parquet input for 2020-11-11
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          QUERY_PLANS_PATH,
          calculateCampaignsRevenueSqlQuery,
          2020,
          11,
          11,
          "Task #2.1 with partitioned parquet input for 2020-11-11"
        )
      }
      case "t3-parquet-engagement-day" => {
        task2CalcMetricsForDayWithParquetInput(   // the most popular channel that drives the highest amount of unique sessions (engagements) with partitioned parquet input for 2020-11-11
          spark,
          CLICKSTREAM_PARQUET_DATA_PATH,
          PURCHASES_PARQUET_DATA_PATH,
          QUERY_PLANS_PATH,
          channelsEngagementPerformanceSqlQuery,
          2020,
          11,
          11,
          "Task #2.2 with partitioned parquet input for 2020-11-11"
        )
      }
      case _ => log.error("Given metric type argument value doesn't match any of available options")
    }

    spark.close()
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

  private def task1TopCsvInputPerformance(spark: SparkSession, clickstreamInput: String, purchasesInput: String): Unit = {
    time {
      val clickStreamDataCsv = readCsv(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataCsv = readCsv(spark, purchasesInput, purchasesDataSchema)

      val df = generatePurchasesAttributionProjection(clickStreamDataCsv, purchasesDataCsv)

      df.show(truncate = false) // force an action
    }("Task #1 with Csv input")
  }

  private def task1UDAFTopCsvInputPerformance(spark: SparkSession, clickstreamInput: String, purchasesInput: String): Unit = {
    time {
      val clickStreamDataCsv = readCsv(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataCsv = readCsv(spark, purchasesInput, purchasesDataSchema)

      val df = generatePurchasesAttributionProjectionWithUDAF(clickStreamDataCsv, purchasesDataCsv)

      df.show(truncate = false)
    }("Task #1 UDAF with Csv input")
  }

  private def task1ParquetInputPerformance(spark: SparkSession, clickstreamInput: String, purchasesInput: String): Unit = {
    time {
      val clickStreamDataParquet = readParquet(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesInput, purchasesDataSchema)

      val df = generatePurchasesAttributionProjection(clickStreamDataParquet, purchasesDataParquet)

      df.show(truncate = false)
    }("Task #1 with Parquet input")
  }

  private def task1UDAFParquetInputPerformance(spark: SparkSession, clickstreamInput: String, purchasesInput: String): Unit = {
    time {
      val clickStreamDataParquet = readParquet(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesInput, purchasesDataSchema)

      val df = generatePurchasesAttributionProjectionWithUDAF(clickStreamDataParquet, purchasesDataParquet)

      df.show(truncate = false)
    }("Task #1 UDAF with Parquet input")
  }

  private def task2CalcMetricsWithCsvInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, f: String => String, logMessage: String): Unit = {
    time {
      val clickStreamDataCsv = readCsv(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataCsv = readCsv(spark, purchasesInput, purchasesDataSchema)

      calculate(spark, clickStreamDataCsv, purchasesDataCsv, f)
    }(logMessage)
  }

  private def task2CalcMetricsWithParquetInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, f: String => String, logMessage: String): Unit = {
    time {
      val clickStreamDataParquet = readParquet(spark, clickstreamInput, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesInput, purchasesDataSchema)

      calculate(spark, clickStreamDataParquet, purchasesDataParquet, f)
    }(logMessage)
  }

  private def task2CalcMetricsForYearAndMonthWithCsvInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, queryPlansOutput: String, f: String => String, yearNumber: Int, monthNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickStreamDataCsv = readCsv(spark, clickstreamInput, clickStreamDataSchema)
        .filter(
          year(col(COL_EVENT_TIME)) === yearNumber &&
          month(col(COL_EVENT_TIME)) === monthNumber
        )

      val purchasesDataCsv = readCsv(spark, purchasesInput, purchasesDataSchema)
        .filter(
          year(col(COL_PURCHASE_TIME)) === yearNumber &&
          month(col(COL_PURCHASE_TIME)) === monthNumber
        )

      calculate(spark, clickStreamDataCsv, purchasesDataCsv, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$queryPlansOutput/$logMessage.md")
  }

  private def task2CalcMetricsForYearAndMonthWithParquetInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, queryPlansOutput: String, f: String => String, yearNumber: Int, monthNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickstreamPath = s"$clickstreamInput/year=$yearNumber/month=$monthNumber/*"
      val purchasesPath = s"$purchasesInput/year=$yearNumber/month=$monthNumber/*"

      val clickStreamDataParquet = readParquet(spark, clickstreamPath, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesPath, purchasesDataSchema)

      calculate(spark, clickStreamDataParquet, purchasesDataParquet, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$queryPlansOutput/$logMessage.md")
  }

  private def task2CalcMetricsForDayWithCsvInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, queryPlansOutput: String, f: String => String, yearNumber: Int, monthNumber: Int, dayNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickStreamDataCsv = readCsv(spark, clickstreamInput, clickStreamDataSchema)
        .filter(
          year(col(COL_EVENT_TIME)) === yearNumber &&
          month(col(COL_EVENT_TIME)) === monthNumber &&
          dayofmonth(col(COL_EVENT_TIME)) === dayNumber
        )

      val purchasesDataCsv = readCsv(spark, purchasesInput, purchasesDataSchema)
        .filter(
          year(col(COL_PURCHASE_TIME)) === yearNumber &&
          month(col(COL_PURCHASE_TIME)) === monthNumber &&
          dayofmonth(col(COL_PURCHASE_TIME)) === dayNumber
        )

      calculate(spark, clickStreamDataCsv, purchasesDataCsv, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$queryPlansOutput/$logMessage.md")
  }

  private def task2CalcMetricsForDayWithParquetInput(spark: SparkSession, clickstreamInput: String, purchasesInput: String, queryPlansOutput: String, f: String => String, yearNumber: Int, monthNumber: Int, dayNumber: Int, logMessage: String): Unit = {
    val result = time {
      val clickstreamPath = s"$clickstreamInput/year=$yearNumber/month=$monthNumber/day=$dayNumber"
      val purchasesPath = s"$purchasesInput/year=$yearNumber/month=$monthNumber/day=$dayNumber"

      val clickStreamDataParquet = readParquet(spark, clickstreamPath, clickStreamDataSchema)
      val purchasesDataParquet = readParquet(spark, purchasesPath, purchasesDataSchema)

      calculate(spark, clickStreamDataParquet, purchasesDataParquet, f)
    }(logMessage)

    println("\nLogical plan: \n" + result.queryExecution.logical.toString())
    println("\nPhysical plan: \n" + result.queryExecution.executedPlan.toString())

    saveToDisk(result.queryExecution.toString(), s"$queryPlansOutput/$logMessage.md")
  }

  private def calculate(spark: SparkSession, clickstreamDf: DataFrame, purchasesDf: DataFrame, f: String => String): DataFrame = {
    val summedDf = generatePurchasesAttributionProjection(clickstreamDf, purchasesDf)

    val viewName = s"summedDf"
    summedDf.createOrReplaceTempView(viewName)
    val result = spark.sql(f(viewName))

    result.show(truncate = false)
    result
  }
}
