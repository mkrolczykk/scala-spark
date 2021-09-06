package org.example
package task2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, expr, round, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

object ChannelsStatistics {
  // INPUT
  private val INPUT_DATA_PATH = "src/main/resources/task1_result/*"
  // OUTPUT
  private val BIGGEST_REVENUE_RESULT_WRITE_PATH = "src/main/resources/task2_results/biggest_revenue/"
  private val MOST_POPULAR_CHANNEL_RESULT_WRITE_PATH = "src/main/resources/task2_results/most_popular_channel/"

  private val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Calculate Marketing Campaigns And Channels Statistics")
      .master("local")
      .getOrCreate()
  spark.sparkContext.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {
    val targetDF = readParquet(spark, INPUT_DATA_PATH, workDFSchema).cache()  // Build Purchases Attribution Projection dataframe

    /**
     * SQL version
     */
    val biggestRevenue = calculateCampaignsRevenueSql(targetDF)
    val mostPopularChannel = channelsEngagementPerformanceSql(targetDF)
    /**
     * dataframe API version
     */
//    val biggestRevenue = calculateCampaignsRevenueDf(targetDF)
//    val mostPopularChannel = channelsEngagementPerformanceDf(targetDF)

    writeAsParquet(biggestRevenue, BIGGEST_REVENUE_RESULT_WRITE_PATH)
    writeAsParquet(mostPopularChannel, MOST_POPULAR_CHANNEL_RESULT_WRITE_PATH)
    spark.close()
  }

  private def calculateCampaignsRevenueSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(calculateCampaignsRevenueSqlQuery(viewName))
  }

  def calculateCampaignsRevenueSqlQuery(viewName: String): String = {
    s"""
      SELECT campaignId, round(AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x), 2) as revenue
      FROM (
        SELECT campaignId, collect_list(billingCost) as billings
        FROM (
          SELECT *
          FROM $viewName
          WHERE $viewName.isConfirmed == TRUE
        )
        GROUP BY campaignId
      )
      ORDER BY revenue DESC LIMIT 10
    """
  }

  /**
   * additional alternative implementation of task #2.1 by using Spark Scala DataFrame API
   * @param summed -> Build Purchases Attribution Projection dataframe
   * @return Top 10 marketing campaigns that bring the biggest revenue
   */
  def calculateCampaignsRevenueDf(summed: DataFrame): DataFrame = {
    summed
      .filter(col("isConfirmed") === true)
      .groupBy("campaignId")
      .agg(collect_list(col("billingCost")).alias("billings"))
      .select(col("campaignId"),
        round(expr("AGGREGATE(billings, DOUBLE(0), (acc, x) -> acc + x)"), 2).alias("revenue"))
      .orderBy(col("revenue").desc)
      .limit(10)
  }

  private def channelsEngagementPerformanceSql(summed: DataFrame): DataFrame = {
    val viewName = s"summed"
    summed.createOrReplaceTempView(viewName)
    spark.sql(channelsEngagementPerformanceSqlQuery(viewName))
  }

  def channelsEngagementPerformanceSqlQuery(viewName: String): String = {
    s"""
      SELECT campaignId, channelId, unique_sessions
      FROM (
        SELECT campaignId, channelId, unique_sessions, ROW_NUMBER() OVER(PARTITION BY campaignId ORDER BY unique_sessions DESC) as rnk
        FROM (
          SELECT campaignId, channelId, COUNT(*) as unique_sessions
          FROM $viewName
          GROUP BY campaignId, channelId
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
    val window = Window.partitionBy("campaignId").orderBy(col("count").desc)

    summed
      .groupBy("campaignId", "channelId")
      .count()
      .withColumn("row", row_number().over(window))
      .filter(col("row") === 1).drop("row")
      .withColumnRenamed("count", "unique_sessions")
  }
}
