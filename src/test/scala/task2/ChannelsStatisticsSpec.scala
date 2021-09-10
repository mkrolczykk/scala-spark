package org.example
package task2

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite
import task1.{COL_CAMPAIGN_ID, _}
import task2.ChannelsStatistics._


class ChannelsStatisticsSpec extends AnyFunSuite with LocalSparkSession with DataFrameTestUtils {
  import spark.implicits._

  lazy val sampleDf: DataFrame = {
    Seq(
      ("p1", "2019-01-01 00:01:05", "100.5", "true", "1", "cmp1", "Google Ads"),
      ("p2", "2019-01-01 00:03:10", "200.0", "true", "2", "cmp1", "Yandex Ads"),
      ("p3", "2019-01-01 01:12:15", "300.0", "false", "3", "cmp1", "Google Ads"),
      ("p4", "2019-01-01 02:13:05", "50.2", "true", "4", "cmp2", "Yandex Ads"),
      ("p5", "2019-01-01 02:15:05", "75.0", "true", "4", "cmp2", "Yandex Ads"),
      ("p6", "2019-01-02 13:03:00", "99.0", "false", "5", "cmp2", "Yandex Ads")
    ).toDF(targetDfSchema.fields.toSeq.map(_.name): _*)
      .withColumn(COL_PURCHASE_TIME, col(COL_PURCHASE_TIME).cast(TimestampType))
      .withColumn(COL_BILLING_COST, col(COL_BILLING_COST).cast(DoubleType))
      .withColumn(COL_IS_CONFIRMED, col(COL_IS_CONFIRMED).cast(BooleanType))
  }

  test("top 10 marketing campaigns that bring the biggest revenue by sql") {
    val expectedResult = Seq(
      ("cmp1", 300.5),
      ("cmp2", 125.2),
    ).toDF(COL_CAMPAIGN_ID, COL_REVENUE)

    val viewName = s"summed"
    sampleDf.createOrReplaceTempView(viewName)

    val resDf = spark.sql(calculateCampaignsRevenueSqlQuery(viewName))
    val rowTop1 = resDf.first()
    val rowTop2 = resDf.head(2)(1)

    assert(rowTop1(0) == "cmp1" && rowTop1(1) == 300.5)
    assert(rowTop2(0) == "cmp2" && rowTop2(1) == 125.2)
    assert(assertData(resDf, expectedResult))
  }

  test("top 10 marketing campaigns that bring the biggest revenue by using spark scala dataframe API") {
    val expectedResult = Seq(
      ("cmp1", 300.5),
      ("cmp2", 125.2),
    ).toDF(COL_CAMPAIGN_ID, COL_REVENUE)

    val resDf = calculateCampaignsRevenueDf(sampleDf)
    val rowTop1 = resDf.first()
    val rowTop2 = resDf.head(2)(1)

    assert(rowTop1(0) == "cmp1" && rowTop1(1) == 300.5)
    assert(rowTop2(0) == "cmp2" && rowTop2(1) == 125.2)
    assert(assertData(resDf, expectedResult))
  }

  test("most popular channel that drives the highest amount of unique sessions in each campaign by sql") {
    val expectedResult = Seq(
      ("cmp1", "Google Ads", 2),
      ("cmp2", "Yandex Ads", 3)
    ).toDF(COL_CAMPAIGN_ID, COL_CHANNEL_ID, COL_UNIQUE_SESSIONS)

    val viewName = s"summed"
    sampleDf.createOrReplaceTempView(viewName)
    val resDf = spark.sql(channelsEngagementPerformanceSqlQuery(viewName))

    assert(assertData(resDf, expectedResult))
  }

  test("most popular channel that drives the highest amount of unique sessions in each campaign by using spark scala dataframe API") {
    val expectedResult = Seq(
      ("cmp1", "Google Ads", 2),
      ("cmp2", "Yandex Ads", 3)
    ).toDF(COL_CAMPAIGN_ID, COL_CHANNEL_ID, COL_UNIQUE_SESSIONS)

    val resDf = channelsEngagementPerformanceDf(sampleDf)

    assert(assertData(resDf, expectedResult))
  }
}
