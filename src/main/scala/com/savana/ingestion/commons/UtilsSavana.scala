
package com.savana.ingestion
package commons

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.functions.{col, length, lit, split}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

class UtilsSavana extends CommonsSavana {
  val log: Logger = LogManager.getRootLogger
  val spark: SparkSession = SparkSession.builder().getOrCreate()

  /**
    * Parses array-like data in string format and returns it as a valid Spark array.
    *
    * @param df              DataFrame with the column to fix
    * @param columnWithArray Name of the column to trim and turn into an array
    * @param sep             Separator of the values in the array
    * @return DataFrame with the original columns + columnWithArray + _reduced (same as original but without the initial
    *         and final [] characters) and columnWithArray + _fixed with the final result as an spark array.
    */
  def fixArrayColumn(df: DataFrame, columnWithArray: String, sep: String): DataFrame = {
    df.withColumn(columnWithArray + "_reduced", col(columnWithArray).substr(lit(2), length(col(columnWithArray)) - 2))
      .withColumn(columnWithArray + "_fixed", split(col(columnWithArray + "_reduced"), sep))
  }

  /**
    * Selects the given columns for the selected base table, removes duplicates and writes the output with the given name.
    *
    * @param baseTable  Name of the base table (apparitions or concepts)
    * @param cols       Columns to select
    * @param outputName Name of the final table
    */
  def selectAndWrite(baseTable: String, cols: Seq[Column], outputName: String): Unit = {
    val df = spark.read.table(baseTable).select(cols: _*).distinct()
    df.write.mode("append")
      .format("csv")
      .option("header", value = true)
      .save(Path.outputTable + outputName)

  }

  /**
    * Joins the two base tables, selects and removes duplicates. Saves the table with the given name.
    *
    * @param joinColumn Name of the column to join ("concept_id")
    * @param cols       Columns to select
    * @param outputName Name of the final table
    */
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

