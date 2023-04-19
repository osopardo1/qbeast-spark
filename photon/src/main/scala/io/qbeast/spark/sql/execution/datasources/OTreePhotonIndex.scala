package io.qbeast.spark.sql.execution.datasources

import io.qbeast.spark.sql.execution.{PhotonQueryManager, QueryOperators}
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Expression, GenericInternalRow}
import org.apache.spark.sql.execution.datasources.{
  InMemoryFileIndex,
  PartitionDirectory
}
import org.apache.spark.sql.types.StructType

import java.net.URI

/**
 * FileIndex to prune files
 *
 * @param index the Tahoe log file index
 */
case class OTreePhotonIndex(
    sparkSession: SparkSession,
    snapshot: QbeastPhotonSnapshot,
    options: Map[String, String],
    queryOperators: QueryOperators,
    userSpecifiedSchema: Option[StructType] = None)
    extends InMemoryFileIndex(
      sparkSession = SparkSession.active,
      rootPathsSpecified = Seq(new Path(snapshot.path)),
      parameters = options,
      userSpecifiedSchema = userSpecifiedSchema) {

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(snapshot.path, p)
    }
  }

  /**
   * List Files with pushdown filters
   * @param partitionFilters
   * @param dataFilters
   * @return
   */
  override def listFiles(
      partitionFilters: Seq[Expression],
      dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {

    // Use PhotonQueryManager to filter the files
    val qbeastBlocks = PhotonQueryManager.query(queryOperators, snapshot)

    // Convert QbeastBlocks into FileStatus
    val fileStats = qbeastBlocks.map { b =>
      new FileStatus(
        /* length */ b.size,
        /* isDir */ false,
        /* blockReplication */ 0,
        /* blockSize */ 1,
        /* modificationTime */ b.modificationTime,
        absolutePath(b.path))
    }.toArray

    // Return a PartitionDirectory
    Seq(PartitionDirectory(new GenericInternalRow(Array.empty[Any]), fileStats))

  }

  /**
   * Return the paths of the files
   * @return
   */

  override def inputFiles: Array[String] = {
    listFiles(Seq.empty, Seq.empty)
      .flatMap(_.files)
      .map(f => absolutePath(f.getPath.toString).toString)
      .toArray
  }

  override def refresh(): Unit = {}

  override def sizeInBytes: Long = snapshot.allBlocks().map(_.size).sum

  override def partitionSchema: StructType = StructType(Seq.empty)
}
