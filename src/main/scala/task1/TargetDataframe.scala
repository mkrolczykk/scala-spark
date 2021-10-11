package org.example
package task1

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.{Aggregator, Window}
import org.apache.spark.sql.types._
import com.typesafe.config.ConfigFactory

import java.nio.file.Paths

object TargetDataframe extends Task {
  override val taskId: String = "task1"

  override def checkTaskInput(args: Array[String]): Unit = {
    if (!(args.length > 0)) {
      println("Not enough arguments - ending program")
      System.exit(0)
    }
  }

  /** Main function */
  def createTargetDataframe(args: Array[String]): Unit = {
    checkTaskInput(args)

    val configFile = Paths.get(args(0)).toFile
    val config = ConfigFactory.parseFile(configFile).getConfig(s"$appConfig.$taskId")
    val sparkConfig: Map[String, String] = getSparkConfig(config)

    val spark: SparkSession =
      SparkSession
        .builder()
        .appName("Create Build Purchases Attribution Projection")
        .master(config.getString("master"))
        .getOrCreate()
    spark.sparkContext.setLogLevel(config.getString("log-level"))
    sparkConfig.foreach {item => spark.conf.set(item._1, item._2)}

    val clickStreamDataDF = readCsv(spark, config.getString("clickstream-data-input"), clickStreamDataSchema)
    val purchasesDataDF = readCsv(spark, config.getString("purchases-data-input"), purchasesDataSchema)

    val resultDF = generatePurchasesAttributionProjection(clickStreamDataDF, purchasesDataDF)
    // UDAF version
//    val resultDF = generatePurchasesAttributionProjectionWithUDAF(clickStreamDataDF, purchasesDataDF)

    writeAsParquet(resultDF, config.getString("result-output"))

    spark.close()
  }

  /*
    Task #1.1
  */
  def generatePurchasesAttributionProjection(clickStreamData: DataFrame, purchasesDataDF: DataFrame): DataFrame = {
    val w1 = Window.partitionBy(COL_USER_ID).orderBy(COL_EVENT_TIME)
    val w2 = Window.partitionBy(COL_SESSION_ID)

    val clickStreamDataDF = clickStreamData
      .filter(col(COL_ATTRIBUTES).isNotNull)
      .withColumn("sessionStart",
        when(
          col(COL_EVENT_TYPE) === EventType.appOpen.toString,
          monotonically_increasing_id() + 1
        ).otherwise(lit(0))
      )
      .withColumn(COL_SESSION_ID, sum("sessionStart").over(w1).cast(StringType))
      .withColumn("allAttributes", collect_list(COL_ATTRIBUTES).over(w2))
      .dropDuplicates(COL_SESSION_ID)
      .withColumn("campaign_details", col("allAttributes")(0))
      .withColumn("purchase", explode(expr("slice(allAttributes, 2, SIZE(allAttributes))")))
      .withColumn(COL_CAMPAIGN_ID, get_json_object(col("campaign_details"), "$.campaign_id"))
      .withColumn(COL_CHANNEL_ID, get_json_object(col("campaign_details"), "$.channel_id"))
      .withColumn("purchase_id", get_json_object(col("purchase"), "$.purchase_id"))
      .select(
        col(COL_USER_ID),
        col(COL_SESSION_ID),
        col(COL_CAMPAIGN_ID),
        col(COL_CHANNEL_ID),
        col("purchase_id")
      )

    clickStreamDataDF
      .join(broadcast(purchasesDataDF), clickStreamDataDF("purchase_id") <=> purchasesDataDF(COL_PURCHASE_ID), "inner")
      .select(
        col(COL_PURCHASE_ID),
        col(COL_PURCHASE_TIME),
        col(COL_BILLING_COST),
        col(COL_IS_CONFIRMED),
        col(COL_SESSION_ID),
        col(COL_CAMPAIGN_ID),
        col(COL_CHANNEL_ID),
      )
  }
  /*
     Task #1.2 - Implement Purchases Attribution Projection by using a custom Aggregator or UDAF
  */
  def generatePurchasesAttributionProjectionWithUDAF(clickStreamData: DataFrame, purchasesDataDF: DataFrame): DataFrame = {
    val sumUDAF = udaf(sumAgg)
    val valuesUDAF = udaf(valuesAgg)
    val w1 = Window.partitionBy(COL_USER_ID).orderBy(COL_EVENT_TIME)
    val w2 = Window.partitionBy(COL_SESSION_ID)

    val clickStreamDataDF = clickStreamData
      .filter(col(COL_ATTRIBUTES).isNotNull)
      .withColumn("sessionStart",
        when(
          col(COL_EVENT_TYPE) === EventType.appOpen.toString,
          monotonically_increasing_id() + 1
        ).otherwise(lit(0))
      )
      .withColumn("sessionId", sumUDAF(col("sessionStart")).over(w1))
      .withColumn("allAttributes", valuesUDAF(col(COL_ATTRIBUTES)).over(w2))
      .dropDuplicates(COL_SESSION_ID)
      .withColumn("campaign_details", col("allAttributes")(0))
      .withColumn("purchase", explode(expr("slice(allAttributes, 2, SIZE(allAttributes))")))
      .withColumn(COL_CAMPAIGN_ID, get_json_object(col("campaign_details"), "$.campaign_id"))
      .withColumn(COL_CHANNEL_ID, get_json_object(col("campaign_details"), "$.channel_id"))
      .withColumn("purchase_id", get_json_object(col("purchase"), "$.purchase_id"))
      .select(
        col(COL_USER_ID),
        col(COL_SESSION_ID),
        col(COL_CAMPAIGN_ID),
        col(COL_CHANNEL_ID),
        col("purchase_id")
      )

    clickStreamDataDF
      .join(broadcast(purchasesDataDF), clickStreamDataDF("purchase_id") <=> purchasesDataDF(COL_PURCHASE_ID), "inner")
      .select(
        col(COL_PURCHASE_ID),
        col(COL_PURCHASE_TIME),
        col(COL_BILLING_COST),
        col(COL_IS_CONFIRMED),
        col(COL_SESSION_ID),
        col(COL_CAMPAIGN_ID),
        col(COL_CHANNEL_ID),
      )
  }

  val sumAgg: Aggregator[Long, Long, String] = new Aggregator[Long, Long, String]() {
    override def zero: Long = 0L

    override def reduce(b: Long, a: Long): Long = b + a

    override def merge(b1: Long, b2: Long): Long = b1 + b2

    override def finish(reduction: Long): String = reduction.toString

    override def bufferEncoder: Encoder[Long] = implicitly(ExpressionEncoder[Long])

    override def outputEncoder: Encoder[String] = implicitly(ExpressionEncoder[String])

  }

  val valuesAgg: Aggregator[String, Array[String], Array[String]] = new Aggregator[String, Array[String], Array[String]]() {
    override def zero: Array[String] = Array.empty

    override def reduce(b: Array[String], a: String): Array[String] = b :+ a

    override def merge(b1: Array[String], b2: Array[String]): Array[String] = b1 ++ b2

    override def finish(reduction: Array[String]): Array[String] = reduction

    override def bufferEncoder: Encoder[Array[String]] = implicitly(ExpressionEncoder[Array[String]])

    override def outputEncoder: Encoder[Array[String]] = implicitly(ExpressionEncoder[Array[String]])
  }
}
