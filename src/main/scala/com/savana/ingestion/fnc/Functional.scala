
package org.svn.ingestion
package com.savana.ingestion.fnc

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
    println("FUNCTIONAL PRINT")
  }
  def fncTable2Creation(): Unit ={}

}
