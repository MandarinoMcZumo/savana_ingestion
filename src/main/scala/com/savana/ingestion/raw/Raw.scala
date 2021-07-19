package com.savana.ingestion.raw

import org.apache.spark.sql.functions.{col, datediff, explode}

class Raw() extends RawCols {

    /**
      * Method to execute different execution steps
      */
    def execution(): Unit ={
      val rawFunctions = Seq("apparitionsRawData", "conceptsRawData")
      rawFunctions.par.foreach(function => this call function)
    }

  def apparitionsRawData(): Unit = {
    val apparitions = spark.read.option("header", "true")
      .schema(Schema.apparition)
      .format("csv")
      .load(Path.inputTable + Files.apparitions)

    val apparitionsWithAge = apparitions
      .withColumn("patient_days_age", datediff(col("document_date"), col("birthdate")))

    apparitionsWithAge.write.format("csv").mode("Overwrite").saveAsTable("apparitions")

  }

  def conceptsRawData(): Unit = {
    val concepts = spark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.inputTable + Files.concepts)

    val conceptsFixedChildren = fixArrayColumn(concepts, Col.directChildren, Com.escapedPipe)
    val conceptsFixed = fixArrayColumn(conceptsFixedChildren, Col.directParents, Com.escapedPipe)

    val flattenConcepts = conceptsFixed
      .withColumn(Col.directChildren + "_flat", explode(col(Col.directChildren + "_fixed")))
      .withColumn(Col.directParents + "_flat", explode(col(Col.directParents + "_fixed")))

    flattenConcepts.write.format("parquet").mode("Overwrite").saveAsTable("concepts")
  }


  }
