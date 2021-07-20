import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession

//scala.tools.nsc.Properties.versionString
//val filePath = "C:/t/0MATERIAL_ATTR.CSV"
val a_filePath = "C:/svn/apparitions_table.csv"
val c_filePath = "C:/svn/concepts_table.csv"
//C:/t/sales_with_errors_in_separator.csv_WITH_PIPE


val spark: SparkSession =
  SparkSession.builder().master("local")
    .appName("spark session")
    .config("spark.sql.shuffle.partitions", "3")
    .getOrCreate()

val apparitions = spark.read.csv(a_filePath)
val concepts = spark.read.csv(c_filePath)

apparitions.show()
concepts.show()



//import spark.implicits._


//val log: Logger = LogManager.getRootLogger
//
//log.info("cacahue")
//
//log.isTraceEnabled

//  .trace("cositas")

