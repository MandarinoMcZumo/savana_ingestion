
package com.savana.ingestion
package fnc

class Functional() extends FunctionalCols {

//  import spark.implicits._


  /**
    * Method to execute different execution steps
    */
  def execution(): Unit ={
    val functions = Seq("fncTable1Creation", "fncTable2Creation")
    functions.par.foreach(function => this call function)
  }

  def fncTable1Creation(): Unit ={
    log.info("FUNCTIONAL PRINT")
  }
  def fncTable2Creation(): Unit ={}

}
