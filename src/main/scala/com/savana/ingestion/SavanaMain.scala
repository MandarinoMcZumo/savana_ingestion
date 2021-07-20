package com.savana.ingestion

import com.savana.ingestion.commons.CommonsSavana
import com.savana.ingestion.har.Harmonization
import com.savana.ingestion.fnc.Functional
import org.apache.log4j.{Level, Logger}

object SavanaMain extends CommonsSavana {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    //Suppress Spark output
    Logger.getLogger("akka").setLevel(Level.ERROR)
    args match {
      case Array(ExecParams.full) =>
        har()
        fnc()

      case Array(ExecParams.har) =>
        har()
      case Array(ExecParams.fnc) =>
        fnc()
      case _ =>
        logger.error("ERROR: Invalid option.")
        throw new IllegalArgumentException("Wrong arguments. Usage: \n" +
          "SavanaMain full | har | fnc ")
    }
  }

  def har(): Unit = {
    new Harmonization().execution()
  }

  def fnc(): Unit = {
    new Functional().execution()
  }


}
