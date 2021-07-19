import com.savana.ingestion.commons.CommonsSavana
import com.savana.ingestion.SavanaMain.main
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.junit.{BeforeClass, Test}

import java.io.File
import scala.reflect.io.Directory


class TestFullExecution extends CommonsSavana {

  val testSpark: SparkSession =
    SparkSession.builder().master("local")
      .appName("spark session")
      .config("spark.sql.shuffle.partitions", "3")
      .getOrCreate()

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

    val concept = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.concept)

    val document = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.document)

    val patient = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.patient)

    val appliesTo = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.appliesTo)

    val isA = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.isA)

    val isMentioned = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.isMentionedIn)

    val isRelated = testSpark.read.option("header", "true").option("infer_schema", "true")
      .format("csv").load(Path.outputTable + Tables.isRelatedTo)


    val docID: String = "1400000001241646"
    val conceptID: Int = 105
    val patientID: String = "5487589844736575979"

    val rawConceptCount = rawConcepts.count()
    val rawApparitionCount = rawConcepts.count()
    val conceptCount = concept.where(col("concept_id") === conceptID).count()
    val documentCount = document.where(col("document_id") === docID).count()
    val patientCount = patient.where(col("patient_id") === patientID).count()
    val appliesToCount = appliesTo
      .where(col("document_id") === docID && col("patient_id") === patientID)
      .count()
    val isACount = isA.where(col("concept_id") === conceptID).count()
    val isMentionedCount = isMentioned
      .where(col("concept_id") === conceptID && col("document_id") === docID)
      .count()
    val isRelatedCount = isRelated
      .where(col("concept_id")===conceptID && col("patient_id") === patientID)
      .count()

    assert(rawConceptCount > 0)
    assert(rawApparitionCount > 0)
    assert(conceptCount == 1)
    assert(documentCount == 1)
    assert(patientCount == 1)
    assert(appliesToCount == 1)
    assert(isACount == 1)
    assert(isMentionedCount == 1)
    assert(isRelatedCount == 1)


  }

}

object TestFullExecution extends CommonsSavana {

  @BeforeClass
  def testInitialization(): Unit = {
    new Directory(new File("spark-warehouse")).deleteRecursively()
    new Directory(new File(Path.outputTable)).deleteRecursively()

  }

}