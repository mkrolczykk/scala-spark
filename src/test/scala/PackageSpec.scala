package org.example

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, IntegerType, StringType, TimestampType}
import org.scalatest.funsuite.AnyFunSuite

class PackageSpec extends AnyFunSuite with LocalSparkSession {
  import spark.implicits._

  test("checkDfHasColumnOfType return true") {
    val sampleDf = Seq(
      (1, "value", "2019-01-01 00:01:05", "true"),
    ).toDF("id", "col1", "col2", "col3")
      .withColumn("id", col("id").cast(IntegerType))
      .withColumn("col2", col("col2").cast(TimestampType))
      .withColumn("col3", col("col3").cast(BooleanType))

    assert(checkDfHasColumnOfType(sampleDf, "id", IntegerType))
    assert(checkDfHasColumnOfType(sampleDf, "col1", StringType))
    assert(checkDfHasColumnOfType(sampleDf, "col2", TimestampType))
    assert(checkDfHasColumnOfType(sampleDf, "col3", BooleanType))
  }

  test("checkDfHasColumnOfType return false") {
    val sampleDf = Seq(
      (1, "value", "2019-01-01 00:01:05", "true"),
    ).toDF("id", "col1", "col2", "col3")

    assert(!checkDfHasColumnOfType(sampleDf, "id", StringType))
  }

  test("checkColumnsExist return true") {
    val sampleDf = Seq(
      (1, "value", "value2", "value3"),
    ).toDF("id", "col1", "col2", "col3")

    assert(checkColumnsExist(sampleDf, "id", "col1", "col2"))
  }

  test("checkColumnsExist return false") {
    val sampleDf = Seq(
      (1, "value", "value2", "value3"),
    ).toDF("id", "col1", "col2", "col3")

    assert(!checkColumnsExist(sampleDf, "id", "col1", "notExistingCol"))
  }

}
