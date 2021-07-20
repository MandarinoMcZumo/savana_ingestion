
package com.savana.ingestion
package fnc

import com.savana.ingestion.commons.UtilsSavana
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col}

class FunctionalCols extends UtilsSavana {

  val patientCols: Seq[Column] =
    Seq(col("patient_id"),
      col("gender"))

  val documentCols: Seq[Column] =
    Seq(col("document_id"),
      col("document_date"))

  val conceptCols: Seq[Column] =
    Seq(col("concept_id"),
      col("fsn"))


  val relToCols: Seq[Column] =
    Seq(col("concept_id"),
      col("document_date"),
      col("patient_days_age"),
      col("document_id"),
      col("patient_id"))

  val isMentCols: Seq[Column] =
    Seq(col("concept_id"),
      col("document_id"))

  val appliesToCols: Seq[Column] =
    Seq(col("document_id"),
      col("patient_id"))

  val isACols: Seq[Column] =
    Seq(col("concept_id"),
      col(Col.isA))

  val allRelCols: Seq[Column] =
    Seq(col("concept_id"),
      col(Col.directParents + "_fixed"),
      col(Col.directChildren + "_fixed"))
}
