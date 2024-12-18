package io.qbeast.sources

import io.qbeast.spark.utils.MetadataConfig
import io.qbeast.spark.utils.TagUtils
import io.qbeast.QbeastIntegrationTestSpec
import org.apache.spark.sql.delta.DeltaLog

class QbeastDeltaDataSourceTest extends QbeastIntegrationTestSpec {

  "QbeastDeltaDataSource" should "create a relation" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val data = spark.range(10).toDF("id")
    /*
    data.write
      .format("org.apache.spark.sources.QbeastDeltaDataSource")
      .option("qbeast.enabled", "true")
      .option("columnsToIndex", "id")
      .save(tmpDir)

     */
    data.write
      .format("delta")
      .option("qbeast.enabled", "true")
      .option("columnsToIndex", "id")
      .save(tmpDir)

    val deltaLog = DeltaLog.forTable(spark, tmpDir)
    val snapshot = deltaLog.update()
    val metadata = snapshot.metadata
    println("METADATA: " + metadata.configuration.toString)

    val tags = deltaLog.update().allFiles.map(_.tags).collect()
    println("TAGS")
    tags.foreach(println)

    val indexedDF = spark.read.format("delta").load(tmpDir)
    assertSmallDatasetEquality(data, indexedDF, ignoreNullable = true, orderedComparison = false)
    metadata.configuration.keys should contain(MetadataConfig.revision)
    tags.foreach(fileTags => fileTags.keys should contain(TagUtils.blocks))

  })

}
