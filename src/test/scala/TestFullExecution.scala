package org.svn.ingestion


import com.savana.ingestion.commons.CommonsSavana

import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.Test


class TestFullExecution extends CommonsSavana {

  val spark: SparkSession =
    SparkSession.builder().master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

  //import spark.implicits._
  val log: Logger = LogManager.getRootLogger


  @Test
  def fullExecution(): Unit = {
//    spark.read.table("")
//    main(Array(ExecParams.full))

//    println(scala.tools.nsc.Properties.versionString)
    //val filePath = "C:/t/0MATERIAL_ATTR.CSV"
    val filePath = "C:/t/sales_with_errors_in_separator.csv_WITH_PIPE"


    val spark: SparkSession =
      SparkSession.builder().master("local")
        .appName("spark session")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()

    val df = spark.read.options(Map("delimiter"->"|")).csv(filePath)

    df.show()
  }


}
