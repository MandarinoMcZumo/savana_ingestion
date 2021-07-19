package com.savana.ingestion

import com.savana.ingestion.commons.CommonsSavana
import com.savana.ingestion.raw.Raw
import com.savana.ingestion.har.Harmonization
import org.apache.log4j.{Level, Logger}

object SavanaMain extends CommonsSavana {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    //Suppress Spark output
    Logger.getLogger("akka").setLevel(Level.ERROR)
    args match {
      case Array(ExecParams.full) =>
        raw()
        harmonization()

      case Array(ExecParams.raw) =>
        raw()
      case Array(ExecParams.har) =>
        harmonization()
      case _ =>
        logger.error("ERROR: Invalid option.")
        throw new IllegalArgumentException("Wrong arguments. Usage: \n" +
          "TemplateMain full | raw | har ")
    }
  }

  def raw(): Unit = {
    new Raw().execution()
  }

  def harmonization(): Unit = {
    new Harmonization().execution()
  }


}
