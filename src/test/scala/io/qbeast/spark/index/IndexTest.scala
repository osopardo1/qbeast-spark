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
package io.qbeast.spark.index

import io.qbeast.context.QbeastContext
import io.qbeast.core.model._
import io.qbeast.core.model.BroadcastTableChanges
import io.qbeast.QbeastIntegrationTestSpec
import io.qbeast.TestClasses.Client3
import io.qbeast.TestClasses.Client4
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class IndexTest
    extends AnyFlatSpec
    with Matchers
    with QbeastIntegrationTestSpec
    with IndexTestChecks {

  // TEST CONFIGURATIONS
  private val options = Map("columnsToIndex" -> "age,val2", "cubeSize" -> "10000")
  private def qbeastOptions = QbeastOptions(options)

  private def createDF(spark: SparkSession, numRecords: Int = 100000): DataFrame = {
    import spark.implicits._
    spark
      .range(numRecords)
      .map(i => Client3(i * i, s"student-$i", i.intValue(), i * 1000 + 123, i * 2567.3432143))
      .toDF()
  }

  // Check correctness
  "Indexing method" should "respect the size of the data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF(spark)
        val rev =
          SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, qbeastOptions)

        val (indexed, _) = oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)

        checkDFSize(indexed, df)
      }
    }
  }

  it should "not miss any cube" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF(spark)
        val rev =
          SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, qbeastOptions)

        val (_, tc: BroadcastTableChanges) =
          oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)

        checkCubes(tc.cubeWeightsBroadcast.value)
      }
    }
  }

  it should "respect the weight of the fathers" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF(spark)
        val rev =
          SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, qbeastOptions)

        val (_, tc: BroadcastTableChanges) =
          oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)

        checkWeightsIncrement(tc.cubeWeightsBroadcast.value)
      }
    }
  }

  it should "add only leaves to indexed data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val df = createDF(spark)
        val rev =
          SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, qbeastOptions)

        val (indexed, tc: BroadcastTableChanges) =
          oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)

        checkCubesOnData(tc.cubeWeightsBroadcast.value, indexed, dimensionCount = 2)
      }
    }
  }

  it should "work with real data" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      {
        val inputPath = "src/test/resources/"
        val file1 = "ecommerce100K_2019_Oct.csv"
        val df = spark.read
          .format("csv")
          .option("header", "true")
          .option("inferSchema", "true")
          .load(inputPath + file1)
          .distinct()
        val options =
          QbeastOptions(Map("columnsToIndex" -> "user_id,product_id", "cubeSize" -> "10000"))
        val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)
        val (indexed, tc: BroadcastTableChanges) =
          oTreeAlgorithm.index(df, IndexStatus(rev), options)
        val weightMap = tc.cubeWeightsBroadcast.value

        checkDFSize(indexed, df)
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)

      }
    }
  }

  it should "maintain correctness on append" in withQbeastContextSparkAndTmpDir {
    (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF(spark)

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val qbeastSnapshot = getQbeastSnapshot(tmpDir)

        val offset = 0.5
        val appendData = df
          .withColumn("age", (col("age") * offset).cast(IntegerType))
          .withColumn("val2", (col("val2") * offset).cast(LongType))

        val (indexed, tc: BroadcastTableChanges) =
          oTreeAlgorithm.index(appendData, qbeastSnapshot.loadLatestIndexStatus, qbeastOptions)
        val weightMap = tc.cubeWeightsBroadcast.value

        checkDFSize(indexed, df)
        checkCubes(weightMap)
        checkWeightsIncrement(weightMap)
        checkCubesOnData(weightMap, indexed, 2)

      }

  }

  it should "support null values" in withSparkAndTmpDir { (spark, tmpDir) =>
    val clients = Seq(
      Client4(0, "student-0", Some(0), Some(123), Some(4.5)),
      Client4(1, "student-1", Some(6), Some(789), Some(0.1)),
      Client4(2, "student-2", None, None, None))
    val rdd = spark.sparkContext.makeRDD(clients)
    spark
      .createDataFrame(rdd)
      .write
      .format("qbeast")
      .mode("overwrite")
      .option("columnsToIndex", "age,val2")
      .save(tmpDir)

    val anotherClients = Seq(
      Client4(3, "student-3", Some(2), Some(345), Some(6.7)),
      Client4(4, "student-4", None, None, None),
      Client4(3, "student-5", Some(3), Some(349), Some(10.5)))
    val anotherRdd = spark.sparkContext.makeRDD(anotherClients)
    spark
      .createDataFrame(anotherRdd)
      .write
      .format("qbeast")
      .mode("append")
      .option("columnsToIndex", "age,val2")
      .save(tmpDir)
  }

  it should "follow the rule of children's minWeight >= parent's maxWeight" in
    withQbeastContextSparkAndTmpDir { (spark, tmpDir) =>
      withOTreeAlgorithm { oTreeAlgorithm =>
        val df = createDF(spark)
        val rev =
          SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, qbeastOptions)
        val (_, tc) = oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)

        df.write
          .format("qbeast")
          .mode("overwrite")
          .options(options)
          .save(tmpDir)

        val tableId = new QTableID(tmpDir)
        val snapshot = QbeastContext.metadataManager.loadSnapshot(tableId)
        val blocks = snapshot.loadLatestIndexFiles.collect().flatMap(_.blocks)

        blocks.foreach { block =>
          block.cubeId.parent match {
            case None => // cube is root
            case Some(parent) =>
              val parentMaxWeight = tc.cubeWeight(parent).get
              block.minWeight should be >= parentMaxWeight
          }
        }
      }
    }

  it should "index correctly when a small cubeSize is given" in withSpark { spark =>
    withOTreeAlgorithm { oTreeAlgorithm =>
      val df = createDF(spark, 1000)
      val options =
        QbeastOptions(Map("columnsToIndex" -> "age,val2", "cubeSize" -> "10"))
      val rev = SparkRevisionFactory.createNewRevision(QTableID("test"), df.schema, options)

      val (indexed, tc: BroadcastTableChanges) =
        oTreeAlgorithm.index(df, IndexStatus(rev), qbeastOptions)
      val weightMap = tc.cubeWeightsBroadcast.value

      checkDFSize(indexed, df)
      checkCubes(weightMap)
      checkWeightsIncrement(weightMap)
      checkCubesOnData(weightMap, indexed, 2)
    }
  }

  it should "work with appends that are always out of range" in withSparkAndTmpDir(
    (spark, tmpDir) => {
      var offset = 10000
      spark
        .range(offset)
        .toDF("id")
        .write
        .mode("append")
        .format("qbeast")
        .option("columnsToIndex", "id")
        .option("cubeSize", "100")
        .save(tmpDir)
      val appendSize = 10
      val numAppends = 10
      (1 to numAppends).foreach { _ =>
        val df = spark.range(offset, offset + appendSize).toDF("id")
        df.write.mode("append").format("qbeast").save(tmpDir)
        offset += appendSize
      }
    })

}
