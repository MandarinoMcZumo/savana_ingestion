
package com.savana.ingestion
package commons

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, length, lit, split}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class UtilsSavana extends CommonsSavana {
  val log: Logger = LogManager.getRootLogger
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def fixArrayColumn(df: DataFrame, columnWitArray: String, sep: String): DataFrame = {
    df.withColumn(columnWitArray + "_reduced", col(columnWitArray).substr(lit(2), length(col(columnWitArray)) - 2))
      .withColumn(columnWitArray + "_fixed", split(col(columnWitArray + "_reduced"), sep))
  }

  def selectAndWrite(baseTable: String, cols: Seq[Column], outputName: String): Unit = {
    val df = spark.read.table(baseTable).select(cols: _*).distinct()
    df.write.mode("append")
      .format("csv")
      .option("header", value = true)
      .save(Path.outputTable + outputName)

  }

  def joinSelectAndWrite(joinColumn: Seq[String], cols: Seq[Column], outputName: String): Unit = {
    val apparitions = spark.read.table(Tables.apparitions)
    val concepts = spark.read.table(Tables.concepts)

    val joinedDF = apparitions.join(concepts, joinColumn, "inner").select(cols: _*).distinct()
    joinedDF.write.mode("append")
      .format("csv")
      .option("header", value = true)
      .save(Path.outputTable + outputName)

  }
}

