
package org.svn.ingestion
package com.savana.ingestion.commons

trait EnumsSavana {

  object ExecParams extends Enumeration {
    val full: String = Value("full").toString
    val har: String = Value("har").toString
    val fnc: String = Value("fnc").toString
    val rpt: String = Value("rpt").toString
  }

  object Tables extends Enumeration {
  }

  object Raw extends Enumeration {
  }

  /*
  object Har extends Enumeration {
    val customers: String = Value(har(Regions.master_data).concat("customers")).toString
  }

  object Fnc extends Enumeration {
    val customers: String = Value(fnc(Regions.master_data).concat("customers")).toString
  }

  object Rpt extends Enumeration {
    val customers: String = Value(rpt(Regions.master_data).concat("customers")).toString
  }
  */

}
