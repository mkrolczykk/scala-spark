package org.example
package task1

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.functions.{col, udaf}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, TimestampType}

import task1.TargetDataframe.{generatePurchasesAttributionProjection, generatePurchasesAttributionProjectionWithUDAF, sumAgg, valuesAgg}

class TargetDataframeSpec extends AnyFunSuite with LocalSparkSession with DataFrameTestUtils {
  import spark.implicits._

  lazy val samplePurchasesDF: DataFrame =
    Seq(
      ("p1", "2019-01-01 0:01:05", "100.5", "true"),
      ("p2", "2019-01-01 0:03:10", "200", "true"),
      ("p3", "2019-01-01 1:12:15", "300", "false"),
      ("p4", "2019-01-01 2:13:05", "50.2", "true"),
      ("p5", "2019-01-01 2:15:05", "75", "true"),
      ("p6", "2019-01-02 13:03:00", "99", "false")
    ).toDF(purchasesDataSchema.fields.toSeq.map(_.name): _*)
      .withColumn(COL_PURCHASE_TIME, col(COL_PURCHASE_TIME).cast(TimestampType))
      .withColumn(COL_BILLING_COST, col(COL_BILLING_COST).cast(DoubleType))
      .withColumn(COL_IS_CONFIRMED, col(COL_IS_CONFIRMED).cast(BooleanType))

  lazy val sampleClickstreamDF: DataFrame = {
    Seq(
      ("u1", "u1_e1", "app_open", "2019-01-01 0:00:00", "{'campaign_id': 'cmp1', 'channel_id': 'Google Ads'}"),
      ("u1", "u1_e2", "search_product", "2019-01-01 0:00:05", null),
      ("u1", "u1_e3",	"search_product",	"2019-01-01 0:00:10", null),
      ("u1", "u1_e3",	"search_product",	"2019-01-01 0:00:10", null),
      ("u1", "u1_e5",	"view_product_details",	"2019-01-01 0:00:20", null),
      ("u1", "u1_e6",	"purchase",	"2019-01-01 0:01:00", "{'purchase_id': 'p1'}"),
      ("u1", "u1_e7",	"app_close", "2019-01-01 0:02:00", null),
      ("u2", "u2_e1", "app_open", "2019-01-01 0:00:00",	"{'campaign_id': 'cmp1', 'channel_id': 'Yandex Ads'}"),
      ("u2", "u2_e2", "search_product", "2019-01-01 0:00:03", null),
      ("u2", "u2_e3", "view_product_details", "2019-01-01 0:01:00", null),
      ("u2", "u2_e4", "purchase",	"2019-01-01 0:03:00",	"{'purchase_id': 'p2'}"),
      ("u2", "u2_e5", "app_close", "2019-01-01 0:04:00", null),
      ("u2", "u2_e6", "app_open",	"2019-01-02 0:00:00",	"{'campaign_id': 'cmp2', 'channel_id': 'Yandex Ads'}"),
      ("u2", "u2_e7", "search_product",	"2019-01-02 0:00:03", null),
      ("u2", "u2_e8", "view_product_details",	"2019-01-02 0:01:00", null),
      ("u2", "u2_e10", "app_close",	"2019-01-02 0:04:00", null),
      ("u3", "u3_e1", "app_open",	"2019-01-01 0:00:00",	"{'campaign_id': 'cmp2', 'channel_id': 'Facebook Ads'}"),
      ("u3", "u3_e2", "search_product",	"2019-01-01 0:00:10", null),
      ("u3", "u3_e3", "view_product_details",	"2019-01-01 0:00:30", null),
      ("u3", "u3_e4", "app_close", "2019-01-01 0:02:00", null),
      ("u3", "u3_e5", "app_open",	"2019-01-01 1:11:11",	"{'campaign_id': 'cmp1', 'channel_id': 'Google Ads'}"),
      ("u3", "u3_e6", "search_product",	"2019-01-01 1:11:20", null),
      ("u3", "u3_e7", "view_product_details",	"2019-01-01 1:11:40", null),
      ("u3", "u3_e8", "purchase",	"2019-01-01 1:12:00",	"{'purchase_id': 'p3'}"),
      ("u3", "u3_e9", "app_close", "2019-01-01 1:12:30", null),
      ("u3", "u3_e10", "app_open", "2019-01-02 2:00:00", "{'campaign_id': 'cmp2', 'channel_id': 'Yandex Ads'}"),
      ("u3", "u3_e11", "search_product", "2019-01-02 2:00:06", null),
      ("u3", "u3_e12", "search_product", "2019-01-02 2:00:20", null),
      ("u3", "u3_e13", "view_product_details", "2019-01-02 2:11:15", null),
      ("u3", "u3_e14", "purchase", "2019-01-02 2:13:00", "{'purchase_id': 'p4'}"),
      ("u3", "u3_e15", "search_product", "2019-01-02 2:14:00", null),
      ("u3", "u3_e16", "view_product_details", "2019-01-02 2:14:15", null),
      ("u3", "u3_e17", "purchase", "2019-01-02 2:15:00", "{'purchase_id': 'p5'}"),
      ("u3", "u3_e18", "app_close", "2019-01-02 2:15:40", null),
      ("u3", "u3_e19", "app_open", "2019-01-02 13:00:10", "{'campaign_id': 'cmp2',  'channel_id': 'Yandex Ads'}"),
      ("u3", "u3_e20", "search_product", "2019-01-02 13:00:25", null),
      ("u3", "u3_e21", "view_product_details", "2019-01-02 13:01:20", null),
      ("u3", "u3_e22", "purchase", "2019-01-02 13:03:00", "{'purchase_id': 'p6'}"),
      ("u3", "u3_e23", "app_close",	"2019-01-02 13:06:00", null)
    ).toDF(clickStreamDataSchema.fields.toSeq.map(_.name): _*)
      .withColumn(COL_EVENT_TIME, col(COL_EVENT_TIME).cast(TimestampType))
  }

  lazy val expectedDf: DataFrame = {
    Seq(
      ("p1", "2019-01-01 00:01:05", "100.5", "true", "1", "cmp1", "Google Ads"),
      ("p2", "2019-01-01 00:03:10", "200.0", "true", "2", "cmp1", "Yandex Ads"),
      ("p3", "2019-01-01 01:12:15", "300.0", "false", "9", "cmp1", "Google Ads"),
      ("p4", "2019-01-01 02:13:05", "50.2", "true", "15", "cmp2", "Yandex Ads"),
      ("p5", "2019-01-01 02:15:05", "75.0", "true", "15", "cmp2", "Yandex Ads"),
      ("p6", "2019-01-02 13:03:00", "99.0", "false", "22", "cmp2", "Yandex Ads")
    ).toDF(targetDfSchema.fields.toSeq.map(_.name): _*)
      .withColumn(COL_PURCHASE_TIME, col(COL_PURCHASE_TIME).cast(TimestampType))
      .withColumn(COL_BILLING_COST, col(COL_BILLING_COST).cast(DoubleType))
      .withColumn(COL_IS_CONFIRMED, col(COL_IS_CONFIRMED).cast(BooleanType))
  }

  test("dataframe schema correctness") {
    val resDf = generatePurchasesAttributionProjection(sampleClickstreamDF, samplePurchasesDF)

    assert(assertSchema(resDf.schema, targetDfSchema, checkNullable = false))
  }

  test("dataframe transformation data result") {
    val resDf = generatePurchasesAttributionProjection(sampleClickstreamDF, samplePurchasesDF)

    assert(assertData(resDf, expectedDf))
  }

  test("dataframe with custom UDAF schema correctness") {
    val resDf = generatePurchasesAttributionProjectionWithUDAF(sampleClickstreamDF, samplePurchasesDF)

    assert(assertSchema(resDf.schema, targetDfSchema, checkNullable = false))
  }

  test("dataframe with custom UDAF transformation data result") {
    val resDf = generatePurchasesAttributionProjectionWithUDAF(sampleClickstreamDF, samplePurchasesDF)

    assert(assertData(resDf, expectedDf))
  }

  test("user defined sum UDAF result") {
    val sumUDAF = udaf(sumAgg)

    val sampleDf = Seq(
      (1, 2),
      (1, 5),
      (2, 3),
      (2, 1),
      (3, 0),
      (3, 0)
    ).toDF("id", "value")

    val expectedDf = Seq(
      (1, 2, 7),
      (1, 5, 7),
      (2, 3, 4),
      (2, 1, 4),
      (3, 0, 0),
      (3, 0, 0)
    ).toDF("id", "value", "sum").withColumn("sum", col("sum").cast(StringType))

    val resDf = sampleDf.withColumn("sum", sumUDAF(col("value")).over(Window.partitionBy("id")))

    assert(assertData(resDf, expectedDf))
  }

  test("user defined collect values UDAF result") {
    val valuesUDAF = udaf(valuesAgg)

    val sampleDf = Seq(
      (1, "val1"),
      (1, "val2"),
      (2, "val1"),
      (1, "val3"),
      (3, "val1")
    ).toDF("id", "attr")

    val expectedDf = Seq(
      (1, "val1", Array("val1", "val2", "val3")),
      (1, "val2", Array("val1", "val2", "val3")),
      (1, "val3", Array("val1", "val2", "val3")),
      (2, "val1", Array("val1")),
      (3, "val1", Array("val1")),
    ).toDF("id", "attr", "attr_collected")

    val resDf = sampleDf.withColumn("attr_collected", valuesUDAF(col("attr")).over(Window.partitionBy("id")))

    assert(assertData(resDf, expectedDf))
  }
}
