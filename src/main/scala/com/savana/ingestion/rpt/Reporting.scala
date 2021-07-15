
package com.savana.ingestion
package rpt

class Reporting() extends ReportingCols {

  //import spark.implicits._

  /**
    * Method to execute different execution steps
    */
  def execution(): Unit ={
    val functions = Seq("rptTable1Creation", "rptTable2Creation")
    functions.par.foreach(function => this call function)
  }

  def rptTable1Creation(): Unit ={
    log.info("REPORTING PRINT")

  }
  def rptTable2Creation(): Unit ={}

}
