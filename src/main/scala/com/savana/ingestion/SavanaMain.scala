package com.savana.ingestion

import com.savana.ingestion.commons.CommonsSavana
import com.savana.ingestion.fnc.Functional
import com.savana.ingestion.har.Harmonization
import com.savana.ingestion.rpt.Reporting
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object SavanaMain extends CommonsSavana {

  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]) {

    //Suppress Spark output
    Logger.getLogger("akka").setLevel(Level.ERROR)
    args match {
      case Array(ExecParams.full) =>
        harmonization()
        functional()
        reporting()
      case Array(ExecParams.har) =>
        harmonization()
      case Array(ExecParams.fnc) =>
        functional()
      case Array(ExecParams.rpt) =>
        reporting()
      case _ =>
        logger.error("ERROR: Invalid option.")
        throw new IllegalArgumentException("Wrong arguments. Usage: \n" +
          "TemplateMain full | har | fun | rpt")
    }
  }

  def harmonization(): Unit = {
    new Harmonization().execution()
  }

  def functional(): Unit = {
    new Functional().execution()
  }

  def reporting(): Unit = {
    new Reporting().execution()
  }


}
