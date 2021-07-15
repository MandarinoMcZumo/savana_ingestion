
package org.svn.ingestion
package com.savana.ingestion.har

class Harmonization() extends HarmonizationCols {

  //import spark.implicits._


  /**
    * Method to execute different execution steps
    */
  def execution(): Unit ={
    val functions = Seq("harTable1Creation", "harTable2Creation")
    functions.par.foreach(function => this call function)
  }

  def harTable1Creation(): Unit ={
    println("HARMONIZATION PRINT")

  }
  def harTable2Creation(): Unit ={}

}
