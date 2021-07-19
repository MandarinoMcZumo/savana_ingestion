
package com.savana.ingestion
package commons

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.{Column, SparkSession}

class UtilsSavana extends CommonsSavana {
  val log: Logger = LogManager.getRootLogger
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  def selectAndWrite(baseTable: String, cols: Seq[Column], outputName: String): Unit = {
    val df = spark.read.table(baseTable).select(cols: _*).distinct()
    df.write.mode("append").format("csv").option("header", true).save(Path.outputTable + outputName)

  }

  def joinSelectAndWrite(joinColumn: Seq[String], cols: Seq[Column], outputName: String): Unit = {
    val apparitions = spark.read.table("apparitions")
    val concepts = spark.read.table("concepts")

    val joinedDF = apparitions.join(concepts, joinColumn, "inner").select(cols: _*).distinct()
    joinedDF.write.mode("append").format("csv").option("header", true).save(Path.outputTable + outputName)

  }
}

