package org.example
package task2

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, collect_list, expr, round, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

import task1._

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
    val targetDF = readParquet(spark, INPUT_DATA_PATH, targetDfSchema).cache()

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

    biggestRevenue.show(truncate = false)
    mostPopularChannel.show(truncate = false)

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

  private def channelsEngagementPerformanceSql(summed: DataFrame): DataFrame = {
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
