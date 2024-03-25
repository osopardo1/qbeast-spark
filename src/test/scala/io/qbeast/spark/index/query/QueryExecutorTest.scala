package io.qbeast.spark.index.query

import io.qbeast.core.model.AllSpace
import io.qbeast.core.model.Block
import io.qbeast.core.model.CubeId
import io.qbeast.core.model.CubeStatus
import io.qbeast.core.model.IndexStatus
import io.qbeast.core.model.QTableID
import io.qbeast.core.model.Revision
import io.qbeast.core.model.Weight
import io.qbeast.core.model.WeightRange
import io.qbeast.core.transform.EmptyTransformer
import io.qbeast.spark.delta.DeltaQbeastSnapshot
import io.qbeast.spark.delta.IndexFiles
import io.qbeast.spark.QbeastIntegrationTestSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.delta.DeltaLog
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

import scala.collection.immutable.SortedMap

class QueryExecutorTest extends QbeastIntegrationTestSpec with QueryTestSpec {

  behavior of "QueryExecutor"

  it should "find all sample files" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path)

    val diff = allFiles.toSet -- matchFiles.toSet

    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "filter the files according to the weight" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    val filters = Seq(weightFilters(WeightRange(Weight.MinValue, Weight(0.001))))
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "filter the files according to the space" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    val filters = Seq(expr("a >= 2 and a < 10").expr)
    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path)

    matchFiles.size shouldBe <(allFiles.length)
    matchFiles.foreach(file => allFiles should contain(file))

  })

  it should "find all files in different revisions" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val differentRevision =
      source
        .withColumn("a", col("a") * 5)
        .withColumn("c", col("c") * 6)

    writeTestData(differentRevision, Seq("a", "c"), 10000, tmpdir, "append")

    val deltaLog = DeltaLog.forTable(spark, tmpdir)
    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

    // Including the staging revision
    qbeastSnapshot.loadAllRevisions.size shouldBe 3

    val filters = Seq.empty

    val querySpec = new QuerySpecBuilder(filters)
    val queryExecutor = new QueryExecutor(querySpec, qbeastSnapshot)

    val allDeltaFiles = deltaLog.update().allFiles.collect()
    val allFiles = allDeltaFiles.map(_.path)

    val matchFiles = queryExecutor.execute().map(_.file.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 0
    matchFiles.size shouldBe allFiles.length
    matchFiles.toSet shouldBe allFiles.toSet

  })

  it should "skip blocks with maxWeight < weightRange.from" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      val df = createDF(500000, spark).toDF()

      writeTestData(df, Seq("a", "c"), 10000, tmpDir)

      val deltaLog = DeltaLog.forTable(spark, tmpDir)
      val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())

      val weightRange = WeightRange(Weight(3), Weight(5))
      val expressionFilters = weightFilters(weightRange)
      val querySpecBuilder = new QuerySpecBuilder(Seq(expressionFilters))

      val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)

      val allBlocks =
        deltaLog.update().allFiles.collect().map(IndexFiles.fromAddFile(2)).flatMap(_.blocks)

      val matchingBlocks = queryExecutor.execute()

      allBlocks.filter(_.maxWeight < weightRange.from).foreach { block =>
        matchingBlocks should not contain (block)
      }
    })

  it should "handle index with a missing inner cube" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(80000, spark)

    writeTestData(source.toDF(), Seq("a", "c"), 8000, tmpdir)

    val deltaLog = DeltaLog.forTable(spark, tmpdir)

    val qbeastSnapshot = DeltaQbeastSnapshot(deltaLog.update())
    val revision = qbeastSnapshot.loadLatestRevision
    val indexStatus = qbeastSnapshot.loadIndexStatus(revision.revisionID)

    val innerCubesLevel1 =
      indexStatus.cubesStatuses.keys.filter(cube =>
        cube.depth == 1 && indexStatus.cubesStatuses.contains(cube))
    val cubeToRemove = if (innerCubesLevel1.isEmpty) CubeId.root(2) else innerCubesLevel1.head

    val faultyIndexStatus =
      indexStatus.copy(cubesStatuses = indexStatus.cubesStatuses - cubeToRemove)

    val querySpecBuilder = new QuerySpecBuilder(Seq.empty)
    val querySpec = querySpecBuilder.build(revision).head
    val queryExecutor = new QueryExecutor(querySpecBuilder, qbeastSnapshot)
    val matchFiles = queryExecutor
      .executeRevision(querySpec, faultyIndexStatus)
      .map(_.file.path)

    val allFiles = deltaLog.update().allFiles.collect().map(_.path)

    val diff = allFiles.toSet -- matchFiles.toSet
    diff.size shouldBe 1
  })

  it should "find the max value when filtering" in withSparkAndTmpDir((spark, tmpdir) => {
    val source = createDF(50000, spark).toDF().coalesce(4)

    writeTestData(source, Seq("a", "c"), 4000, tmpdir)

    val indexed = spark.read.format("qbeast").load(tmpdir)

    indexed.where("a == 50000").count shouldBe 1
    indexed.where("c == 50000.0").count shouldBe 1
  })

  it should "filter correctly using GreaterThan and LessThanOrEqual" in withSparkAndTmpDir(
    (spark, tmpdir) => {
      val source = createDF(50000, spark).toDF().coalesce(4)

      writeTestData(source, Seq("a", "c"), 4000, tmpdir)

      val indexed = spark.read.format("qbeast").load(tmpdir)

      indexed.where("a > 1").count shouldBe 49999
      indexed.where("a > 49999").count shouldBe 1

      indexed.where("a <= 1").count shouldBe 2
      indexed.where("a <= 49999").count shouldBe 50000

    })

  it should "filter cube blocks by query weight range" in
    withSparkAndTmpDir((spark, tmpDir) => {
      val samplingFraction = 0.5
      val root = CubeId.root(2)
      val c1 = root.firstChild

      val rootBlock =
        s"""{
          "cubeId": "${root.string}",
          "minWeight": ${Weight(0.0).value},
          "maxWeight": ${Weight(0.1).value},
          "elementCount": 1, "replicated": false}""".stripMargin
      val c1Block1 =
        s"""{
          "cubeId": "${c1.string}",
          "minWeight": ${Weight(0.1).value},
          "maxWeight": ${Weight(0.2).value},
          "elementCount": 1,"replicated": false}""".stripMargin
      val c1Block2 =
        s"""{"cubeId": "${c1.string}",
          "minWeight": ${Weight(0.7).value},
          "maxWeight": ${Weight(0.8).value},
          "elementCount": 1, "replicated": false}"""

      val Seq(rb1, c1b1, c1b2) = IndexFiles
        .fromAddFile(2)(
          AddFile(
            path = tmpDir,
            partitionValues = Map.empty,
            size = 1L,
            modificationTime = 1L,
            dataChange = true,
            tags = Map("blocks" -> s"""[$rootBlock, $c1Block1, $c1Block2]""")))
        .blocks

      val indexStatus = IndexStatus(
        Revision.firstRevision(
          new QTableID(tmpDir),
          1,
          Vector(EmptyTransformer("t1"), EmptyTransformer("t2"))),
        cubesStatuses = SortedMap(
          root -> CubeStatus(root, Weight(0.1), 0.1, rb1 :: Nil),
          c1 -> CubeStatus(root, Weight(0.2), 0.2, c1b1 :: c1b2 :: Nil)))

      val executor = new QueryExecutor(
        new QuerySpecBuilder(Seq.empty[Expression]),
        DeltaQbeastSnapshot(DeltaLog.forTable(spark, tmpDir).update()))

      val executeRevision: PrivateMethod[Seq[Block]] = PrivateMethod[Seq[Block]]('executeRevision)
      val outputBlocks = executor invokePrivate executeRevision(
        QuerySpec(WeightRange(Weight(0d), Weight(samplingFraction)), AllSpace()),
        indexStatus)

      // c1Block2 should not be part of outputBlocks
      outputBlocks.size shouldBe 2
      outputBlocks.contains(c1b2) shouldBe false
    })

}
