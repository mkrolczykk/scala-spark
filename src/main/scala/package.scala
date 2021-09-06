package org

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}

package object example {
  /**
   * Common variables
   */
  val CLICKSTREAM_DATA_PATH = "capstone-dataset/mobile_app_clickstream/*.csv.gz"  // path for mobile app clickstream dataset in .csv.gz format
  val PURCHASES_DATA_PATH = "capstone-dataset/user_purchases/*.csv.gz"  // path for purchases projection dataset in .csv.gz format

  /**
   * Schemas
   */
  val clickStreamDataSchema: StructType = StructType(Array(
    StructField("userId", StringType, nullable = false),
    StructField("eventId", StringType, nullable = false),
    StructField("eventType", StringType, nullable = false),
    StructField("eventTime", TimestampType, nullable = false),
    StructField("attributes", StringType, nullable = true),
  ))

  val purchasesDataSchema: StructType = StructType(Array(
    StructField("purchaseId", StringType, nullable = false),
    StructField("purchaseTime", TimestampType, nullable = false),
    StructField("billingCost", DoubleType, nullable = false),
    StructField("isConfirmed", BooleanType, nullable = false),
  ))

  val workDFSchema: StructType = StructType(Array(
    StructField("purchaseId", StringType, nullable = false),
    StructField("purchaseTime", TimestampType, nullable = false),
    StructField("billingCost", DoubleType, nullable = false),
    StructField("isConfirmed", BooleanType, nullable = false),
    StructField("sessionId", StringType, nullable = false),
    StructField("campaignId", StringType, nullable = false),
    StructField("channelId", StringType, nullable = false),
  ))

  /**
   * Functions
   */
  def readCsv(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark
      .read
      .schema(schema)
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .csv(path)
  }

  def readParquet(spark: SparkSession, path: String, schema: StructType): DataFrame = {
    spark
      .read
      .schema(schema)
      .options(Map("header" -> "true", "inferSchema" -> "true"))
      .parquet(path)
  }

  def writeAsParquet(dfToSave: DataFrame, path: String): Unit = dfToSave.write.mode(SaveMode.Overwrite).parquet(path)
}
