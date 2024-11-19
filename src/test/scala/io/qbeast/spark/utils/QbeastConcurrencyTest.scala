package io.qbeast.spark.utils

import io.qbeast.table.QbeastTable
import io.qbeast.QbeastIntegrationTestSpec

class QbeastConcurrencyTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "index a table concurrently appended" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog)((spark, tmpWarehouse) => {

    // Define the test data
    val lastRangeFirstWrite = 10000
    val df1 = spark.range(1, lastRangeFirstWrite).toDF("id") // bigger append

    // Specify the path where data will be written
    val destinationTable = "append_test_table_destination"
    val sourceTable = "append_test_table_source"
    val destinationPath = s"$tmpWarehouse/$destinationTable"
    val sourcePath = s"$tmpWarehouse/$sourceTable"
    val sourceFormat = "qbeast"

    spark.sql(s"""CREATE TABLE $sourceTable (id LONG)
         | USING $sourceFormat LOCATION '$sourcePath'
         | OPTIONS (columnsToIndex 'id', cubeSize '100')""".stripMargin)
    spark.sql(s"""CREATE TABLE $destinationTable (id LONG)
         | USING qbeast LOCATION '$destinationPath'
         | OPTIONS (columnsToIndex 'id', cubeSize '100')""".stripMargin)

    // Define the append function
    def appendDataSource(df: org.apache.spark.sql.DataFrame): Unit = {
      // TODO: appending as delta to avoid MAP problems
      df.write.format("delta").mode("append").save(sourcePath)
    }

    def appendDataDestination(): Unit = {
      spark.sql(s"INSERT INTO $destinationTable SELECT id FROM $sourceTable")
    }

    // First append
    println("FIRST APPEND TO SOURCE")
    appendDataSource(df1)

    // Define thread for append1
    val thread1 = new Thread(new Runnable {
      def run(): Unit = {
        try {
          println("Starting append in DESTINATION...")
          appendDataDestination() // Perform append2
          println("append in DESTINATION completed.")
        } catch {
          case e: Exception =>
            println(s"Error on INSERT INTO: ${e.getStackTrace.mkString("\n")}}")
        }
      }
    })

    // Define thread for append2

    val thread2 = new Thread(new Runnable {
      def run(): Unit = {
        // println("Starting append in SOURCE...")
        val numberOfSmallAppends = 10
        var count = 0
        var lastRange = lastRangeFirstWrite
        while (count < numberOfSmallAppends) {
          // println(s"Appending $count")
          val df = spark.range(lastRange, lastRange + 10).toDF("id")
          try {
            appendDataSource(df) // Perform append1
            count += 1
            lastRange += 10
            // println(s"Appended $count finished")
          } catch {
            case e: Exception =>
              println(s"Error on SMALL APPEND: ${e.getStackTrace.mkString("\n")}}")
              return
          }
        }
        // println("append in SOURCE completed")
      }
    })

    // Start both threads
    thread1.start()
    thread2.start()

    // Wait for both threads to complete
    thread1.join()
    thread2.join()

    // Verify the results
    val resultSourceDF = spark.read.format(sourceFormat).load(sourcePath)
    val resultDestinationDF = spark.read.format("qbeast").load(destinationPath)

    // Check row count to verify both DataFrames were appended
    val expectedRowCount = resultSourceDF.count()
    val actualRowCount = resultDestinationDF.count()

    assert(
      actualRowCount == expectedRowCount,
      s"Test failed! Expected $expectedRowCount rows but found $actualRowCount.")
  })

  it should "Test append" in withQbeastContextSparkAndTmpWarehouse((spark, tmpWarehouse) => {
    val tableLocation = s"$tmpWarehouse/test_append"
    spark.sql(s"""CREATE TABLE test_append (id LONG)
                 |  USING qbeast
                 |  LOCATION '$tableLocation'
                 |  OPTIONS (columnsToIndex 'id', cubeSize '100')""".stripMargin)

    def appendDataSource(df: org.apache.spark.sql.DataFrame): Unit = {
      df.write.format("qbeast").mode("append").save(tableLocation)
    }

    val lastRangeFirstWrite = 10000
    val df = spark.range(1, lastRangeFirstWrite).toDF("id")
    appendDataSource(df)

    val numberOfSmallAppends = 10
    var count = 0
    var lastRange = lastRangeFirstWrite
    while (count < numberOfSmallAppends) {
      println(s"Appending $count")
      val df = spark.range(lastRange, lastRange + 10).toDF("id")
      appendDataSource(df) // Perform append1
      count += 1
      lastRange += 10
      println(s"Appended $count finished")
    }
  })

  it should "work with columnStats" in withQbeastContextSparkAndTmpWarehouse(
    (spark, tmpWarehouse) => {
      val tableLocation = s"$tmpWarehouse/test_column_stats_3"
      spark.sql(s"""CREATE TABLE test_column_stats_4 (date BIGINT)
                   |  USING qbeast
                   |  LOCATION '$tableLocation'
                   |  OPTIONS (
                   |     columnsToIndex = 'date',
                   |     cubeSize = '100000',
                   |     columnStats = '{"date_min": 1719792000000, "date_max": 1735689600000}'
                   |  )""".stripMargin)

      val df = spark.range(1719792000000L, 1719792000030L).toDF("date")
      df.createOrReplaceTempView("tmp")
      spark.sql("INSERT INTO test_column_stats_4 SELECT * FROM tmp")

      val qbeastTable = QbeastTable.forPath(spark, tableLocation)
      val revision = qbeastTable.latestRevision
      val transformations = revision.transformations
      println("TRANSFORMATIONS SHOULD CONTAIN COLUMN STATS")
      transformations.foreach(println)

    })

  it should "simple dataframe testing" in withSparkAndTmpDir((spark, tmpDir) => {
    val deltaPath = tmpDir + "/delta_table"
    val qbeastPath = tmpDir + "/qbeast_table"
    val df = spark.range(1, 1000).toDF("id")

    df.write.format("delta").save(deltaPath)
    df.write.format("qbeast").option("columnsToIndex", "id").save(qbeastPath)
    val deltaLoad = spark.read.format("delta").load(deltaPath)
    val qbeastLoad = spark.read.format("qbeast").load(qbeastPath)

    val df2 = spark.range(1000, 2001).toDF("id")
    df2.write.format("delta").mode("append").save(deltaPath)
    df2.write.format("qbeast").mode("append").save(qbeastPath)

    println("DELTA DF COUNT AFTER APPEND: " + deltaLoad.count())
    println("QBEAST DF COUNT AFTER APPEND: " + qbeastLoad.count())
  })

  it should "simple dataframe testing with SQL" in withSparkAndTmpDir((spark, tmpDir) => {
    val deltaPath = tmpDir + "/delta_table"
    val qbeastPath = tmpDir + "/qbeast_table"
    val parquetPath = tmpDir + "/parquet_table"
    val df = spark.range(1, 1000).toDF("id")
    spark.sql("CREATE TABLE delta_table (id LONG) USING delta LOCATION '" + deltaPath + "'")
    spark.sql(
      "CREATE TABLE qbeast_table (id LONG) USING qbeast OPTIONS (columnsToIndex 'id') LOCATION '" + qbeastPath + "'")
    spark.sql("CREATE TABLE parquet_table (id LONG) USING parquet LOCATION '" + parquetPath + "'")

    df.write.format("delta").mode("append").save(deltaPath)
    df.write.format("qbeast").mode("append").save(qbeastPath)
    df.write.format("parquet").mode("append").save(parquetPath)

    val deltaLoad = spark.sql("SELECT * FROM delta_table")
    val qbeastLoad = spark.sql("SELECT * FROM qbeast_table")
    val parquetLoad = spark.sql("SELECT * FROM parquet_table")

    println("DELTA DF COUNT BEFORE APPEND: " + deltaLoad.count())
    println("QBEAST DF COUNT BEFORE APPEND: " + qbeastLoad.count())
    println("PARQUET DF COUNT BEFORE APPEND: " + parquetLoad.count())

    spark.range(1000, 2001).toDF("id").createOrReplaceTempView("tmp_view")
    spark.sql("INSERT INTO delta_table SELECT * FROM tmp_view")
    spark.sql("INSERT INTO qbeast_table SELECT * FROM tmp_view")
    spark.sql("INSERT INTO parquet_table SELECT * FROM tmp_view")

    deltaLoad.explain(true)

    println("DELTA DF COUNT AFTER APPEND: " + deltaLoad.count())
    println("QBEAST DF COUNT AFTER APPEND: " + qbeastLoad.count())
    println("PARQUET DF COUNT AFTER APPEND: " + parquetLoad.count())

  })

}
