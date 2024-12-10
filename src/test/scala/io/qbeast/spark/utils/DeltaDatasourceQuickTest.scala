package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Student
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession

import scala.util.Random

class DeltaDatasourceQuickTest extends QbeastIntegrationTestSpec {

  private val students = 1.to(10).map(i => Student(i, i.toString, Random.nextInt()))

  private def createStudentsTestData(spark: SparkSession): DataFrame = {
    import spark.implicits._
    students.toDF()
  }

  "The QbeastDataSource" should
    "work with DataFrame API" in withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      {
        import spark.implicits._
        val data = createStudentsTestData(spark)
        data.write.format("io.qbeast.sources.QbeastDataSource").option("columnsToIndex", "id").save(tmpDir)

        val allFilestags = DeltaLog.forTable(spark, tmpDir).update().allFiles.map(_.tags)
        allFilestags.collect().foreach(println)

      }
    }

}
