package org.example
package task2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, expr, round, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}
import task1._

import com.typesafe.config.ConfigFactory

import java.nio.file.Paths

object ChannelsStatistics extends Task {
  override val taskId: String = "task2"

  override def checkTaskInput(args: Array[String]): Unit = {
    if (!(args.length > 0)) {
      println("Not enough arguments - ending program")
      System.exit(0)
    }
  }

  /** Main function */
  def calculateStatistics(args: Array[String]): Unit = {
    checkTaskInput(args)

    val configFile = Paths.get(args(0)).toFile
    val config = ConfigFactory.parseFile(configFile).getConfig(s"$appConfig.$taskId")
    val sparkConfig: Map[String, String] = getSparkConfig(config)

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Calculate Marketing Campaigns And Channels Statistics")
        .master(config.getString("master"))
        .getOrCreate()
    spark.sparkContext.setLogLevel(config.getString("log-level"))
    sparkConfig.foreach {item => spark.conf.set(item._1, item._2)}

    val targetDF = readParquet(spark, config.getString("input-data"), targetDfSchema).cache()

    /**
     * SQL version
    */
    val biggestRevenue = calculateCampaignsRevenueSql(spark, targetDF)
    val mostPopularChannel = channelsEngagementPerformanceSql(spark, targetDF)
    /**
     * dataframe API version
     */
//    val biggestRevenue = calculateCampaignsRevenueDf(targetDF)
//    val mostPopularChannel = channelsEngagementPerformanceDf(targetDF)

    biggestRevenue.show(truncate = false)
    mostPopularChannel.show(truncate = false)

    writeAsParquet(biggestRevenue, config.getString("biggest-revenue-output"))
    writeAsParquet(mostPopularChannel, config.getString("most-popular-channels-output"))
    spark.close()
  }

  private def calculateCampaignsRevenueSql(spark: SparkSession, summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(calculateCampaignsRevenueSqlQuery(viewName))
  }

  def calculateCampaignsRevenueSqlQuery(viewName: String): String = {
    s"""
      SELECT $COL_CAMPAIGN_ID, round(AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x), 2) as $COL_REVENUE
      FROM (
        SELECT $COL_CAMPAIGN_ID, collect_list(billingCost) as billings
        FROM (
          SELECT *
          FROM $viewName AS v
          WHERE v.$COL_IS_CONFIRMED == TRUE
        )
        GROUP BY $COL_CAMPAIGN_ID
      )
      ORDER BY $COL_REVENUE DESC LIMIT $ROW_LIMIT
    """
  }

  /**
   * additional alternative implementation of task #2.1 by using Spark Scala DataFrame API
   * @param summed -> Build Purchases Attribution Projection dataframe
   * @return Top 10 marketing campaigns that bring the biggest revenue
   */
  def calculateCampaignsRevenueDf(summed: DataFrame): DataFrame = {
    summed
      .filter(col(COL_IS_CONFIRMED) === true)
      .groupBy(COL_CAMPAIGN_ID)
      .agg(collect_list(col("billingCost")).alias("billings"))
      .select(
        col(COL_CAMPAIGN_ID),
        round(expr("AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x)"), 2).alias(COL_REVENUE)
      )
      .orderBy(col(COL_REVENUE).desc)
      .limit(ROW_LIMIT)
  }

  private def channelsEngagementPerformanceSql(spark: SparkSession, summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(channelsEngagementPerformanceSqlQuery(viewName))
  }

  def channelsEngagementPerformanceSqlQuery(viewName: String): String = {
    s"""
      SELECT $COL_CAMPAIGN_ID, $COL_CHANNEL_ID, $COL_UNIQUE_SESSIONS
      FROM (
        SELECT $COL_CAMPAIGN_ID, $COL_CHANNEL_ID, $COL_UNIQUE_SESSIONS, ROW_NUMBER() OVER(PARTITION BY $COL_CAMPAIGN_ID ORDER BY $COL_UNIQUE_SESSIONS DESC) as rnk
        FROM (
          SELECT $COL_CAMPAIGN_ID, $COL_CHANNEL_ID, COUNT(*) as $COL_UNIQUE_SESSIONS
          FROM $viewName
          GROUP BY $COL_CAMPAIGN_ID, $COL_CHANNEL_ID
        )
      )
      WHERE rnk == 1
    """
  }

  /**
   * additional alternative implementation of task #2.2 by using Spark Scala DataFrame API
   * @param summed -> Build Purchases Attribution Projection dataframe
   * @return the most popular channel that drives the highest amount of unique sessions (engagements) with the App in each campaign
   */
  def channelsEngagementPerformanceDf(summed: DataFrame): DataFrame = {
    val window = Window.partitionBy(COL_CAMPAIGN_ID).orderBy(col("count").desc)

    summed
      .groupBy(COL_CAMPAIGN_ID, COL_CHANNEL_ID)
      .count()
      .withColumn("row", row_number().over(window))
      .filter(col("row") === 1)
      .withColumnRenamed("count", COL_UNIQUE_SESSIONS)
      .select(
        col(COL_CAMPAIGN_ID),
        col(COL_CHANNEL_ID),
        col(COL_UNIQUE_SESSIONS)
      )
  }
}
