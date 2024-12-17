package io.qbeast.spark.index

import io.qbeast.QbeastIntegrationTestSpec

class ConcurrentSourceTest extends QbeastIntegrationTestSpec {

  "Qbeast" should "throw and error when concurrent sources are used" in withExtendedSparkAndTmpDir(
    sparkConfWithSqlAndCatalog.set("spark.scheduler.mode", "FAIR")) { (spark, tmpDir) =>
    // Define the test data
    val df1 = spark.range(1, 100000).toDF("id") // bigger append

    // Specify the path where data will be written
    val destinationTable = "append_test_table_destination"
    val sourceTable = "append_test_table_source"
    val sourceFormat = "qbeast"
    val destinationPath = s"$tmpDir/destination"
    val sourcePath = s"$tmpDir/source"

    spark.sql(s"""CREATE TABLE $sourceTable (id LONG)
           | USING $sourceFormat LOCATION '$sourcePath'
           | OPTIONS (columnsToIndex 'id', cubeSize '100')""".stripMargin)

    spark.sql(s"""CREATE TABLE $destinationTable (id LONG)
           | USING qbeast LOCATION '$destinationPath'
           | OPTIONS (columnsToIndex 'id', cubeSize '100')""".stripMargin)

    // Define the append function
    def appendDataSource(df: org.apache.spark.sql.DataFrame): Unit = {
      df.write.format(sourceFormat).mode("append").save(sourcePath)
    }

    def appendDataDestinationQbeast(): Unit = {
      spark.sql(
        "INSERT INTO TABLE append_test_table_destination SELECT * FROM append_test_table_source")
    }

    // First append
    println("FIRST APPEND TO SOURCE")
    appendDataSource(df1)

    // Define thread for append1
    val thread1 = new Thread(new Runnable {
      def run(): Unit = {
        try {
          println("Starting append in DESTINATION...")
          appendDataDestinationQbeast() // Perform append2
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
        println("Starting append in SOURCE...")
        val numberOfSmallAppends = 10
        var count = 0
        var lastRange = 100000
        while (count < numberOfSmallAppends) {
          println(s"Appending $count")
          val df = spark.range(lastRange, lastRange + 10).toDF("id")
          try {
            appendDataSource(df) // Perform append1
            count += 1
            lastRange += 10
            println(s"Appended $count finished")
          } catch {
            case e: Exception =>
              println(s"Error on SMALL APPEND: ${e.getStackTrace.mkString("\n")}}")
              return
          }
        }
        println("append in SOURCE completed")
      }
    })

    // Start both threads
    thread1.start()
    thread2.start()

    // Wait for both threads to complete
    thread1.join()
    thread2.join()

    // Verify the results
    val resultSourceDF = spark.read.format("qbeast").load(sourcePath)
    val resultDestinationDF = spark.read.format("qbeast").load(destinationPath)

    // Check row count to verify both DataFrames were appended
    val expectedRowCount = resultSourceDF.count()
    val actualRowCount = resultDestinationDF.count()

    assert(
      actualRowCount == expectedRowCount,
      s"Test failed! Expected $expectedRowCount rows but found $actualRowCount.")
  }

}
