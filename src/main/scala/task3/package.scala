package org.example

import task1.{COL_BILLING_COST, COL_CAMPAIGN_ID, COL_CHANNEL_ID, COL_IS_CONFIRMED, COL_PURCHASE_ID, COL_PURCHASE_TIME, COL_SESSION_ID}

import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

package object task3 {
  // Columns
  val COL_YEAR = "year"
  val COL_QUARTER = "quarter"
  val COL_WEEK_OF_YEAR = "weekOfYear"

  val weeklyPurchasesSchema: StructType = StructType(
    Array(
      StructField(COL_PURCHASE_ID, StringType, nullable = false),
      StructField(COL_PURCHASE_TIME, TimestampType, nullable = false),
      StructField(COL_BILLING_COST, DoubleType, nullable = false),
      StructField(COL_IS_CONFIRMED, BooleanType, nullable = false),
      StructField(COL_SESSION_ID, StringType, nullable = false),
      StructField(COL_CAMPAIGN_ID, StringType, nullable = false),
      StructField(COL_CHANNEL_ID, StringType, nullable = false),
      StructField(COL_YEAR, IntegerType, nullable = false),
      StructField(COL_QUARTER, IntegerType, nullable = false),
      StructField(COL_WEEK_OF_YEAR, IntegerType, nullable = false)
    )
  )
}
