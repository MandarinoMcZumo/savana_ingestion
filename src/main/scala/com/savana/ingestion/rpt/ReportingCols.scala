
package org.svn.ingestion
package com.savana.ingestion.rpt

import com.savana.ingestion.commons.UtilsSavana
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class ReportingCols extends UtilsSavana {

  val table1Fields: Seq[Column] =
    Seq(col("field1"),
      col("field2"))


}
