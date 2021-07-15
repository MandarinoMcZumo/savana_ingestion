
package com.savana.ingestion
package commons

trait EnumsSavana {

  object ExecParams extends Enumeration {
    val full: String = Value("full").toString
    val har: String = Value("com/savana/ingestion/har").toString
    val fnc: String = Value("com/savana/ingestion/fnc").toString
    val rpt: String = Value("com/savana/ingestion/rpt").toString
  }

  object Tables extends Enumeration {
  }

  object Raw extends Enumeration {
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
