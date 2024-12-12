package io.qbeast.spark.utils

import io.qbeast.QbeastIntegrationTestSpec

class QbeastConcurrentSourceTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "throw error when underlying source changes" in withSparkAndTmpDir {
    (spark, tmpDir) =>
      // Given
      val source = "src/test/resources/parquet"
      val qbeast = "src/test/resources/qbeast"
      val partitions = 1
      val threads = 2
      val error = intercept[Exception] {
        // When
        spark.range(10).write.format("parquet").save(source)
      }
      // Then
      assert(error.getMessage === "Indexing underlying source concurrently is not supported")
  }

}
