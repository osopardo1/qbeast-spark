package io.qbeast

import io.qbeast.spark.delta.DefaultFileIndex
import io.qbeast.spark.internal.expressions.QbeastMurmur3Hash
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.DataFrame

object TestUtils extends QbeastIntegrationTestSpec {

  def checkLogicalFilterPushdown(sqlFilters: Seq[String], query: DataFrame): Unit = {
    val leaves = query.queryExecution.sparkPlan.collectLeaves()

    val dataFilters = leaves
      .collectFirst {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[DefaultFileIndex] =>
          f.dataFilters.filterNot(_.isInstanceOf[QbeastMurmur3Hash])
      }
      .getOrElse(Seq.empty)

    val dataFiltersSql = dataFilters.map(_.sql)
    sqlFilters.foreach(filter => dataFiltersSql should contain(filter))
  }

  def checkFiltersArePushedDown(query: DataFrame): Unit = {
    val leaves =
      query.queryExecution.executedPlan.collectLeaves().filter(_.isInstanceOf[FileSourceScanExec])

    leaves should not be empty

    leaves.exists(p =>
      p
        .asInstanceOf[FileSourceScanExec]
        .relation
        .location
        .isInstanceOf[DefaultFileIndex]) shouldBe true

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[DefaultFileIndex] =>
          f.dataFilters.nonEmpty shouldBe true
      }
  }

  def checkFileFiltering(query: DataFrame): Unit = {
    val leaves =
      query.queryExecution.executedPlan.collectLeaves().filter(_.isInstanceOf[FileSourceScanExec])

    leaves should not be empty

    leaves.exists(p =>
      p
        .asInstanceOf[FileSourceScanExec]
        .relation
        .location
        .isInstanceOf[DefaultFileIndex]) shouldBe true

    leaves
      .foreach {
        case f: FileSourceScanExec if f.relation.location.isInstanceOf[DefaultFileIndex] =>
          val index = f.relation.location
          val matchingFiles =
            index.listFiles(f.partitionFilters, f.dataFilters).flatMap(_.files)
          val allFiles = index.listFiles(Seq.empty, Seq.empty).flatMap(_.files)
          matchingFiles.length shouldBe <(allFiles.length)
      }

  }

}
