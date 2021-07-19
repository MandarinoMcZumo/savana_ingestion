
package com.savana.ingestion
package commons

import org.apache.spark.sql.types.{StringType, StructField, StructType, ArrayType, IntegerType}

trait EnumsSavana {
  object Path extends Enumeration {
    val inputTable: String = Value("C:/svn/").toString
    val outputTable: String = Value("C:/svn_output/").toString
    val raw: String = outputTable + "raw/"
    val har: String = outputTable + "har/"
    val fnc: String = outputTable + "fnc/"

  }

  object Com extends Enumeration {
    val empty: String = ""
  }

  object Col extends Enumeration {
    val directParents: String = Value("direct_parents").toString
    val directChildren: String = Value("direct_children").toString

  }

  object ExecParams extends Enumeration {
    val full: String = Value("full").toString
    val har: String = Value("har").toString
    val fnc: String = Value("fnc").toString
    val rpt: String = Value("rpt").toString
  }

  object Tables extends Enumeration {
    val apparitions: String = Value("apparitions_table.csv").toString
    val concepts: String = Value("concepts_table.csv").toString
  }

  object Schema extends Enumeration {
    val concept = StructType(Array(
      StructField("concept_id", StringType, true),
      StructField("direct_parents", ArrayType(IntegerType), true),
      StructField("direct_children", ArrayType(IntegerType), true),
      StructField("fsn", StringType, true))
    )
    val apparition = StructType(Array(
      StructField("document_date", StringType, true),
      StructField("patient_id", StringType, true),
      StructField("gender", StringType, true),
      StructField("birthdate", StringType, true),
      StructField("document_id", StringType, true),
      StructField("concept_id", StringType, true))
    )

  }

  /*
  object Har extends Enumeration {
    val customers: String = Value(com.savana.ingestion.har(Regions.master_data).concat("customers")).toString
  }

  object Fnc extends Enumeration {
    val customers: String = Value(com.savana.ingestion.fnc(Regions.master_data).concat("customers")).toString
  }

  object Rpt extends Enumeration {
    val customers: String = Value(com.savana.ingestion.rpt(Regions.master_data).concat("customers")).toString
  }
  */

}
