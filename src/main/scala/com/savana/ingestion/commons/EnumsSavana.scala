
package com.savana.ingestion
package commons

import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait EnumsSavana {
  object Path extends Enumeration {
    val inputTable: String = Value("src/test/resources/").toString
    val outputTable: String = Value("src/test/resources/output/").toString
    val raw: String = outputTable + "raw/"
    val har: String = outputTable + "har/"

  }

  object Com extends Enumeration {
    val empty: String = ""
    val escapedPipe: String = "\\|"
  }

  object Col extends Enumeration {
    val directParents: String = Value("direct_parents").toString
    val directChildren: String = Value("direct_children").toString
    val isA: String = Value("is_a").toString

  }

  object ExecParams extends Enumeration {
    val full: String = Value("full").toString
    val har: String = Value("har").toString
    val fnc: String = Value("fnc").toString
  }

  object Files extends Enumeration {
    val apparitions: String = Value("apparitions_table.csv").toString
    val concepts: String = Value("concepts_table.csv").toString
  }

  object Tables extends Enumeration {
    val apparitions: String = Value("apparitions").toString
    val concepts: String = Value("concepts").toString
    val patient: String = Value("patient").toString
    val document: String = Value("document").toString
    val concept: String = Value("concept").toString
    val isARelationship: String = Value("is_a_relationship").toString
    val isRelatedTo: String = Value("is_related_to").toString
    val isMentionedIn: String = Value("is_mentioned_in").toString
    val appliesTo: String = Value("applies_to").toString
    val isA: String = Value("is_a").toString

  }

  object Schema extends Enumeration {

    val apparition: StructType = StructType(Array(
      StructField("document_date", StringType, true),
      StructField("patient_id", StringType, true),
      StructField("gender", StringType, true),
      StructField("birthdate", StringType, true),
      StructField("document_id", StringType, true),
      StructField("concept_id", StringType, true))
    )

  }

}
