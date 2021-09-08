package org

import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.example.task3.CalculateMetrics.log

package object example {
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

  // Build Purchases Attribution Projection dataframe schema
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
   * Common functions
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

  def writeAsParquetWithPartitioning(toSave: DataFrame, path: String, partitionCols: String*): Unit = {
    if (partitionCols.forall(toSave.columns.toList.contains)) {
      toSave
        .write
        .mode(SaveMode.Overwrite)
        .partitionBy(partitionCols: _*)
        .parquet(path)
    } else {
      log.error(s"Write as parquet with given partition columns operation failed - not all of given '$partitionCols' columns exist")
      log.warn("Saving data with partitioning by default")
      writeAsParquet(toSave, path)
    }
  }

  def checkColumnCorrectness(df: DataFrame, colName: String, colType: String): Boolean = {
    if(df.columns.contains(colName) && df.schema(colName).dataType.typeName == colType) true else false
  }

  def checkPathExists(path: String): Boolean = if(new java.io.File(path).exists) true else false
}
