
package com.savana.ingestion
package fnc

import org.apache.spark.sql.functions._

class Functional() extends FunctionalCols {


  /**
    * Method to execute different execution steps
    */
  def execution(): Unit = {
    val fncFunctions = Seq("patient",
      "document",
      "concept",
      "isARelationship",
      "isRelatedTo",
      "isMentionedIn",
      "appliesTo")
    fncFunctions.par.foreach(fncFunction => this call fncFunction)

  }


  def patient(): Unit = {
    selectAndWrite(Tables.apparitions, patientCols, Tables.patient)
  }

  def document(): Unit = {
    selectAndWrite(Tables.apparitions, documentCols, Tables.document)
  }

  def concept(): Unit = {
    selectAndWrite(Tables.concepts, conceptCols, Tables.concept)
  }

  /**
    * Selects, filters and saves all the relationships between concepts
    */
  def isARelationship(): Unit = {
    val baseRel = spark.read.table(Tables.concepts).select(allRelCols: _ *)

    val isA = baseRel
      .withColumn(Col.isA, explode(col(Col.directParents + "_fixed")))
      .select(isACols: _ *)
      .where(col(Col.isA) =!= Com.empty)


    isA.write.mode("append")
      .format("csv")
      .option("header", true)
      .save(Path.outputTable + Tables.isA)
  }

  def isRelatedTo(): Unit = {
    joinSelectAndWrite(Seq("concept_id"), relToCols, Tables.isRelatedTo)
  }

  def isMentionedIn(): Unit = {
    joinSelectAndWrite(Seq("concept_id"), isMentCols, Tables.isMentionedIn)
  }

  def appliesTo(): Unit = {
    selectAndWrite(Tables.apparitions, appliesToCols, Tables.appliesTo)
  }


}
