package com.savana.ingestion.har

import org.apache.spark.sql.functions.{col, datediff, explode}

class Harmonization() extends HarmonizationCols {

  /**
    * Method to execute different execution steps
    */
  def execution(): Unit = {
    val harFunctions = Seq("apparitionsRawData", "conceptsRawData")
    harFunctions.par.foreach(function => this call function)
  }

  /**
    * Method to read the apparitions raw file and calculate the field patient_days_age
    */
  def apparitionsRawData(): Unit = {
    val apparitions = spark.read.option("header", "true")
      .schema(Schema.apparition)
      .format("csv")
      .load(Path.inputTable + Files.apparitions)

    val apparitionsWithAge = apparitions
      .withColumn("patient_days_age", datediff(col("document_date"), col("birthdate")))

    apparitionsWithAge.write.format("csv").mode("Overwrite").saveAsTable(Tables.apparitions)

  }

  /**
    * Method to read the concepts raw file, fix the array values (fields direct_children and direct_parents
    * removing the initial and ending [] and turning the data type into a Spark Array
    */
  def conceptsRawData(): Unit = {
    val concepts = spark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.inputTable + Files.concepts)

    val conceptsFixedChildren = fixArrayColumn(concepts, Col.directChildren, Com.escapedPipe)
    val conceptsFixed = fixArrayColumn(conceptsFixedChildren, Col.directParents, Com.escapedPipe)

    conceptsFixed.write.format("parquet").mode("Overwrite").saveAsTable(Tables.concepts)
  }


}

