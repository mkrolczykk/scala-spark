package org

import com.typesafe.config.Config
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType, StructField, StructType, TimestampType}
import org.example.task1._

import scala.collection.JavaConverters._

package object example {
  val log: Logger = LogManager.getRootLogger
  log.setLevel(Level.ERROR)
  /**
   * Schemas
   */
  val clickStreamDataSchema: StructType = StructType(Array(
    StructField(COL_USER_ID, StringType, nullable = false),
    StructField(COL_EVENT_ID, StringType, nullable = false),
    StructField(COL_EVENT_TYPE, StringType, nullable = false),
    StructField(COL_EVENT_TIME, TimestampType, nullable = false),
    StructField(COL_ATTRIBUTES, StringType, nullable = true),
  ))

  val purchasesDataSchema: StructType = StructType(Array(
    StructField(COL_PURCHASE_ID, StringType, nullable = false),
    StructField(COL_PURCHASE_TIME, TimestampType, nullable = false),
    StructField(COL_BILLING_COST, DoubleType, nullable = false),
    StructField(COL_IS_CONFIRMED, BooleanType, nullable = false),
  ))

  // Build Purchases Attribution Projection dataframe schema
  val targetDfSchema: StructType = StructType(Array(
    StructField(COL_PURCHASE_ID, StringType, nullable = false),
    StructField(COL_PURCHASE_TIME, TimestampType, nullable = false),
    StructField(COL_BILLING_COST, DoubleType, nullable = false),
    StructField(COL_IS_CONFIRMED, BooleanType, nullable = false),
    StructField(COL_SESSION_ID, StringType, nullable = false),
    StructField(COL_CAMPAIGN_ID, StringType, nullable = false),
    StructField(COL_CHANNEL_ID, StringType, nullable = false),
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
    if (checkDfHasColumnOfType(dfToSave, partitionCol, TimestampType)) {
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
    if (checkColumnsExist(toSave, partitionCols: _*)) {
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

  def checkDfHasColumnOfType(df: DataFrame, colName: String, colType: Any): Boolean = {
    if(df.columns.contains(colName) && df.schema(colName).dataType == colType) true else false
  }

  def checkColumnsExist(df: DataFrame, cols: String*): Boolean = cols.forall(df.columns.toList.contains)

  def checkPathExists(path: String): Boolean = new java.io.File(path).exists

  def getSparkConfig(config: Config, attribute: String = "spark-config"): Map[String, String] = {
    config.getObject(attribute).keySet().asScala.map { key =>
      key -> config.getString(s"$attribute.$key")
    }.toMap
  }

  def checkConfigExists(args: Array[String]): Boolean = if (args.length > 1 && checkPathExists(args(1))) true else false
}
