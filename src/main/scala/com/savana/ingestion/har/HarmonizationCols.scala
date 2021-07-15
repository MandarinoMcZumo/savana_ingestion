
package com.savana.ingestion
package har

import com.savana.ingestion.commons.UtilsSavana
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

class HarmonizationCols extends UtilsSavana {

  val table1Fields: Seq[Column] =
    Seq(col("field1"),
      col("field2"))


}
