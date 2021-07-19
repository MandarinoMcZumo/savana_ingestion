
package com.savana.ingestion
package har

import com.savana.ingestion.commons.UtilsSavana
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col}

class HarmonizationCols extends UtilsSavana {

  val patientCols: Seq[Column] =
    Seq(col("patient_id"),
      col("gender"))

  val documentCols: Seq[Column] =
    Seq(col("document_id"),
      col("document_date"))

  val conceptCols: Seq[Column] =
    Seq(col("concept_id"),
      col("fsn"))

  val isRelCols: Seq[Column] =
    Seq(col(Col.directParents + "_flat"),
      col(Col.directChildren + "_flat"))

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
      col(Col.directParents + "_flat").as(Col.isA))
}
