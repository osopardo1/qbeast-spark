/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.index.model.transformer

import io.qbeast.spark.QbeastIntegrationTestSpec
import io.qbeast.TestClasses._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TransformerIndexingTest extends AnyFlatSpec with Matchers with QbeastIntegrationTestSpec {

  // Write source data indexing all columns and read it back
  private def writeAndReadDF(source: Dataset[_], tmpDir: String, spark: SparkSession) = {
    source.write
      .format("qbeast")
      .option("columnsToIndex", source.columns.mkString(","))
      .option("cubeSize", 10000)
      .save(tmpDir)

    spark.read
      .format("qbeast")
      .load(tmpDir)
  }

  "Qbeast spark" should "Index tables with string" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => T1(i, s"$i", i.toDouble))
      .as[T1]

    source.write
      .format("qbeast")
      .option("columnsToIndex", "a,b,c")
      .option("cubeSize", 10000)
      .save(tmpDir)

    val indexed = spark.read
      .format("qbeast")
      .load(tmpDir)
      .as[T1]

    source.count() shouldBe indexed.count()

    assertSmallDatasetEquality[T1](
      source,
      indexed,
      ignoreNullable = true,
      orderedComparison = false)

  })

  it should
    "index tables with hashing configuration" in withSparkAndTmpDir((spark, tmpDir) => {
      import spark.implicits._
      val source = spark
        .range(100001)
        .map(i => T2(i, i.toDouble))

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a:hashing,c:hashing")
        .option("cubeSize", 10000)
        .save(tmpDir)

      val indexed = spark.read
        .format("qbeast")
        .load(tmpDir)
        .as[T2]

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with all String" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestStrings(s"${i * 2}", s"$i", s"$i$i"))
      .as[TestStrings]
    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestStrings]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with non-numeric string" in withSparkAndTmpDir((spark, tmpDir) => {

    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestStrings(s"some_string$i", s"some_other_string$i", i.toString))
      .as[TestStrings]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestStrings]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Double" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestDouble((i * i).toDouble, i.toDouble, (i * 2).toDouble))
      .as[TestDouble]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestDouble]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Int" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(_.toInt)
      .map(i => TestInt(i * i, i, i * 2))

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestInt]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with BigDecimal" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestBigDecimal(BigDecimal(i * i), BigDecimal(i), BigDecimal(i * 2)))
      .as[TestBigDecimal]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestBigDecimal]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Float" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestFloat(i * i, i.toInt, i * 2))
      .as[TestFloat]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestFloat]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Long" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestLong(i * i, i, i * 2))
      .as[TestLong]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestLong]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Timestamps" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      Seq(
        "2017-01-01 12:02:00",
        "2017-01-02 12:02:00",
        "2017-01-03 12:02:00",
        "2017-01-04 12:02:00").toDF("date")
    val source = df.withColumn("my_date", to_timestamp($"date"))

    val indexed = writeAndReadDF(source, tmpDir, spark)

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with all Dates" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val df =
      Seq("2017-01-01", "2017-01-02", "2017-01-03", "2017-01-04").toDF("date")
    val source = df.withColumn("my_date", to_date($"date"))

    val indexed = writeAndReadDF(source, tmpDir, spark)

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, ignoreNullable = true, orderedComparison = false)

  })

  it should "index tables with multiple rows of a unique Timestamp" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq(
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00",
          "2017-01-01 12:02:00").toDF("date")
      val source = df.withColumn("my_date", to_timestamp($"date"))

      val indexed = writeAndReadDF(source, tmpDir, spark)

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with multiple rows of a unique Date" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val df =
        Seq("2017-01-01", "2017-01-01", "2017-01-01", "2017-01-01").toDF("date")
      val source = df.withColumn("my_date", to_date($"date"))

      val indexed = writeAndReadDF(source, tmpDir, spark)

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(
        source,
        indexed,
        ignoreNullable = true,
        orderedComparison = false)

    })

  it should "index tables with null values" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(_.toInt)
      .map(i =>
        if (i % 2 == 0) TestNull(Some(s"student$i"), None, Some(i * 2))
        else TestNull(Some(s"student$i"), Some(i), Some(i * 2)))
      .as[TestNull]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, orderedComparison = false)

  })

  it should "index tables with ALL null values" in withSparkAndTmpDir((spark, tmpDir) => {
    import spark.implicits._
    val source = spark
      .range(100001)
      .map(i => TestNull(Some(s"student$i"), None, Some(i * 2)))
      .as[TestNull]

    val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

    indexed.count() shouldBe source.count()

    assertSmallDatasetEquality(source, indexed, orderedComparison = false)

  })

  it should "index tables with the same value in all rows" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      import spark.implicits._
      val source = spark
        .range(100000)
        .map(i => TestNull(Some(s"student$i"), Some(10), Some(i)))

      val indexed = writeAndReadDF(source, tmpDir, spark).as[TestNull]

      indexed.count() shouldBe source.count()

      assertSmallDatasetEquality(source, indexed, orderedComparison = false)
    })

  it should "don't miss records when indexing null string" in withSparkAndTmpDir(
    (spark, tmpDir) => {

      // Reproducing a particular Github Archive dataset
      // with all null values in one column
      // and poor cardinality (4 groups) in the other
      import spark.implicits._
      val source = spark
        .range(200000)
        .map(i => TestNull(None, None, Some(i % 4)))

      source.write
        .format("qbeast")
        .option("columnsToIndex", "a,c")
        .option("cubeSize", 10000)
        .save(tmpDir)

      val indexed = spark.read.format("qbeast").load(tmpDir)

      val is_null = """a is null"""
      indexed.where(is_null).count() shouldBe 200000

      (1 to 4).foreach(i => {
        val filter = s"""a is null and c == $i"""
        indexed.where(filter).count() shouldBe source.where(filter).count()
      })

    })

  it should "index table with all bigint" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      import spark.implicits._
      val transformedTimestamps =
        Seq(1727576294425L, 1727573413393L, 1727575303653L, 1727573943159L, 1727574359221L,
          1727559190306L, 1727559235931L, 1727559841937L, 1727559870441L, 1727569279499L).toDF(
          "date")
      val identityTimestamps = Seq.fill(10000)(1729429674924L).toDF("date")

      transformedTimestamps
        .union(identityTimestamps)
        .write
        .format("delta")
        .saveAsTable("allTransformedTimestamps")

      transformedTimestamps.createOrReplaceTempView("transformedTimestamps")

      spark.sql("""CREATE OR REPLACE TABLE test_bigint (value BIGINT)
          |USING qbeast
          |OPTIONS (columnsToIndex 'value', cubeSize '100')""".stripMargin)

      import io.qbeast.spark.QbeastTable
      val tableIdentifier = org.apache.spark.sql.catalyst.TableIdentifier("test_bigint")
      val tableMetadata = spark.sessionState.catalog.getTableMetadata(tableIdentifier)
      val location = tableMetadata.location.toString
      println(s"Table location: $location")
      val qbeastTable = QbeastTable.forPath(spark, location)
      println("REVISION")
      println(qbeastTable.latestRevision.toString)

      spark.sql("INSERT INTO test_bigint SELECT * FROM allTransformedTimestamps LIMIT 1")
      println("REVISION AFTER")
      println(qbeastTable.latestRevision.toString)

      val indexed = spark.sql("SELECT * FROM test_bigint")
      indexed.count() shouldBe 1

    })

}
