import com.savana.ingestion.commons.CommonsSavana
import com.savana.ingestion.SavanaMain.main
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.junit.{BeforeClass, Test}

import java.io.File
import scala.reflect.io.Directory


class TestFullExecution extends CommonsSavana {

  val testSpark: SparkSession =
    SparkSession.builder().master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

  //import spark.implicits._
  val log: Logger = LogManager.getRootLogger


  @Test
  def fullExecution(): Unit = {
    main(Array(ExecParams.full))
    val rawApparitions = testSpark.read.option("header", "true")
      .schema(Schema.apparition)
      .format("csv")
      .load(Path.inputTable + Files.apparitions)

    val rawConcepts = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.inputTable + Files.concepts)



  }

}

object TestFullExecution extends CommonsSavana {

  @BeforeClass
  def testInitialization(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively()
    new Directory(new File(Path.outputTable)).deleteRecursively()

  }

}