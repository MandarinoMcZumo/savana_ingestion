
package com.savana.ingestion
package har

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType}

class Harmonization() extends HarmonizationCols {


  /**
    * Method to execute different execution steps
    */
  def execution(): Unit = {
    val rawFunctions = Seq("apparitionsRawData", "conceptsRawData")
    val harFunctions = Seq("patient", "document", "concept", "isARelationship", "isRelatedTo", "isMentionedIn", "appliesTo")

    rawFunctions.par.foreach(rawFunction => this call rawFunction)
    harFunctions.par.foreach(harFunction => this call harFunction)

  }

  def apparitionsRawData(): Unit = {
    val apparitions = spark.read.option("header", "true")
      .schema(Schema.apparition)
      .format("csv")
      .load(Path.inputTable + Tables.apparitions)

    val apparitionsWithAge = apparitions
      .withColumn("patient_days_age", datediff(col("document_date"), col("birthdate")))

    apparitionsWithAge.write.format("csv").mode("Overwrite").saveAsTable("apparitions")

  }

  def conceptsRawData(): Unit = {
    val concepts = spark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.inputTable + Tables.concepts)
      .withColumn(Col.directChildren + "_reduced", col(Col.directChildren).substr(lit(2), length(col(Col.directChildren)) - 2))
      .withColumn(Col.directChildren + "_fixed", split(col(Col.directChildren + "_reduced"), "\\|"))
      .withColumn(Col.directParents + "_reduced", col(Col.directParents).substr(lit(2), length(col(Col.directParents)) - 2))
      .withColumn(Col.directParents + "_fixed", split(col(Col.directParents + "_reduced"), "\\|"))


    val flattenConcepts = concepts
      .withColumn(Col.directChildren + "_flat", explode(col(Col.directChildren + "_fixed")))
      .withColumn(Col.directParents + "_flat", explode(col(Col.directParents + "_fixed")))

    flattenConcepts.write.format("parquet").mode("Overwrite").saveAsTable("concepts")
  }


  def patient(): Unit = {
    selectAndWrite("apparitions", patientCols, "patient")
  }

  def document(): Unit = {
    selectAndWrite("apparitions", documentCols, "document")
  }

  def concept(): Unit = {
    selectAndWrite("concepts", conceptCols, "concept")
  }

  def isARelationship(): Unit = {
    val rel = spark.read.table("concepts").select(isRelCols: _*)
      .where(col(Col.directParents + "_flat") =!= Com.empty && col(Col.directChildren + "_flat") =!= Com.empty)

    rel.write.mode("append").format("csv").option("header", true).save(Path.outputTable + "isARelationship")
  }

  def isRelatedTo(): Unit = {
    joinSelectAndWrite(Seq("concept_id"), relToCols, "isRelatedTo")
  }

  def isMentionedIn(): Unit = {
    joinSelectAndWrite(Seq("concept_id"), isMentCols, "isMentionedIn")
  }

  def appliesTo(): Unit = {
    selectAndWrite("apparitions", appliesToCols, "appliesTo")
  }


}
