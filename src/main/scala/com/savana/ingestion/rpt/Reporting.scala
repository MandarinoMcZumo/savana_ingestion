
package org.svn.ingestion
package com.savana.ingestion.rpt

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
    println("REPORTING PRINT")

  }
  def rptTable2Creation(): Unit ={}

}
