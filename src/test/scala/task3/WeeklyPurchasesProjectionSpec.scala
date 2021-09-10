package org.example
package task3

import task1.{COL_BILLING_COST, COL_IS_CONFIRMED, COL_PURCHASE_TIME}
import task3.WeeklyPurchasesProjection.{buildWeeklyPurchasesProjectionPerQuarter, parsePath}

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite

class WeeklyPurchasesProjectionSpec extends AnyFunSuite with LocalSparkSession with DataFrameTestUtils {
  import spark.implicits._

  lazy val sampleDf: DataFrame = {
      Seq(
        ("p1", "2019-01-01 00:01:05", "100.5", "true", "1", "cmp1", "Google Ads"),
        ("p2", "2019-05-06 00:03:10", "200.0", "true", "2", "cmp1", "Yandex Ads"),
        ("p3", "2019-07-12 01:12:15", "300.0", "false", "9", "cmp1", "Google Ads"),
        ("p4", "2019-12-28 02:13:05", "50.2", "true", "15", "cmp2", "Yandex Ads"),
      ).toDF(colNames = targetDfSchema.fields.toSeq.map(_.name): _*)
        .withColumn(COL_PURCHASE_TIME, col(COL_PURCHASE_TIME).cast(TimestampType))
        .withColumn(COL_BILLING_COST, col(COL_BILLING_COST).cast(DoubleType))
        .withColumn(COL_IS_CONFIRMED, col(COL_IS_CONFIRMED).cast(BooleanType))
  }

  test("parsed path is correct") {
    val path = "src/main/"
    val expectedQ1Path = s"$path/year=2020/month={1,2,3}/*"
    val expectedQ2Path = s"$path/year=2020/month={4,5,6}/*"
    val expectedQ3Path = s"$path/year=2020/month={7,8,9}/*"
    val expectedQ4Path = s"$path/year=2020/month={10,11,12}/*"
    val expectedAllPath = s"$path/year=2020/month={*}/*"

    assert(parsePath(path, 2020, Quarter.Q1) === expectedQ1Path)
    assert(parsePath(path, 2020, Quarter.Q2) === expectedQ2Path)
    assert(parsePath(path, 2020, Quarter.Q3) === expectedQ3Path)
    assert(parsePath(path, 2020, Quarter.Q4) === expectedQ4Path)
    assert(parsePath(path, 2020, Quarter.ALL) === expectedAllPath)
  }

  test("weekly purchases projection schema") {
    val resDf = buildWeeklyPurchasesProjectionPerQuarter(sampleDf, COL_PURCHASE_TIME)

    assert(assertSchema(resDf.schema, weeklyPurchasesSchema, checkNullable = false))
  }

  test("weekly purchases projection within one quarter data") {
    val expectedDf = {
      Seq(
        ("p1", "2019-01-01 00:01:05", "100.5", "true", "1", "cmp1", "Google Ads", "2019", "1", "1"),
        ("p2", "2019-05-06 00:03:10", "200.0", "true", "2", "cmp1", "Yandex Ads", "2019", "2", "19"),
        ("p3", "2019-07-12 01:12:15", "300.0", "false", "9", "cmp1", "Google Ads", "2019", "3", "28"),
        ("p4", "2019-12-28 02:13:05", "50.2", "true", "15", "cmp2", "Yandex Ads", "2019", "4", "52")
      ).toDF(colNames = weeklyPurchasesSchema.fields.toSeq.map(_.name): _*)
        .withColumn(COL_PURCHASE_TIME, col(COL_PURCHASE_TIME).cast(TimestampType))
        .withColumn(COL_BILLING_COST, col(COL_BILLING_COST).cast(DoubleType))
        .withColumn(COL_IS_CONFIRMED, col(COL_IS_CONFIRMED).cast(BooleanType))
        .withColumn(COL_YEAR, col(COL_YEAR).cast(IntegerType))
        .withColumn(COL_QUARTER, col(COL_QUARTER).cast(IntegerType))
        .withColumn(COL_WEEK_OF_YEAR, col(COL_WEEK_OF_YEAR).cast(IntegerType))
    }

    val resDf = buildWeeklyPurchasesProjectionPerQuarter(sampleDf, COL_PURCHASE_TIME)

    assert(assertData(resDf, expectedDf))
  }

  test("weekly purchases projection function throws NoSuchFieldException because of non existing column") {
    assertThrows[NoSuchFieldException] { buildWeeklyPurchasesProjectionPerQuarter(sampleDf, "nonExistingCol") }
  }

  test("weekly purchases projection function throws NoSuchFieldException because of wrong type column") {
    assertThrows[NoSuchFieldException] { buildWeeklyPurchasesProjectionPerQuarter(sampleDf, COL_BILLING_COST) }
  }
}
