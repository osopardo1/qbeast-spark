/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

import io.qbeast.core.keeper.Keeper
import io.qbeast.core.model._
import io.qbeast.spark.delta.CubeDataLoader
import io.qbeast.spark.index.QbeastColumns
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.internal.sources.QbeastBaseRelation
import org.apache.spark.sql.delta.actions.FileAction
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisExceptionFactory, DataFrame}

/**
 * Indexed table represents the tabular data storage
 * indexed with the OTree indexing technology.
 */
trait IndexedTable {

  /**
   * Returns whether the table physically exists.
   * @return the table physically exists.
   */
  def exists: Boolean

  /**
   * Returns the table id which identifies the table.
   *
   * @return the table id
   */
  def tableID: QTableID

  /**
   * Saves given data in the table and updates the index. The specified columns are
   * used to define the index when the table is created or overwritten. The append
   * flag defines whether the existing data should be overwritten.
   *
   * @param data the data to save
   * @param parameters the parameters to save the data
   * @param append the data should be appended to the table
   * @return the base relation to read the saved data
   */
  def save(data: DataFrame, parameters: Map[String, String], append: Boolean): BaseRelation

  /**
   * Loads the table data.
   *
   * @return the base relation to read the table data
   */
  def load(): BaseRelation

  /**
   * Analyzes the index for a given revision
   * @param revisionID the identifier of revision to analyze
   * @return the cubes to analyze
   */
  def analyze(revisionID: RevisionID): Seq[String]

  /**
   * Optimizes the given table for a given revision
   * @param revisionID the identifier of revision to optimize
   */
  def optimize(revisionID: RevisionID): Unit
}

/**
 * IndexedTable factory.
 */
trait IndexedTableFactory {

  /**
   * Returns a IndexedTable for given SQLContext and path.
   * It is not guaranteed that the returned table physically
   * exists, use IndexedTable#exists attribute to verify it.
   *
   * @param tableId the table path
   * @return the table
   */
  def getIndexedTable(tableId: QTableID): IndexedTable
}

/**
 * Implementation of IndexedTableFactory.
 * @param keeper the keeper
 * @param indexManager the index manager
 * @param metadataManager the metadata manager
 * @param dataWriter the data writer
 * @param revisionBuilder the revision builder
 */
final class IndexedTableFactoryImpl(
    private val keeper: Keeper,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction],
    private val revisionBuilder: RevisionFactory[StructType])
    extends IndexedTableFactory {

  override def getIndexedTable(tableID: QTableID): IndexedTable =
    new IndexedTableImpl(
      tableID,
      keeper,
      indexManager,
      metadataManager,
      dataWriter,
      revisionBuilder)

}

/**
 * Implementation of IndexedTable.
 *
 * @param tableID the table identifier
 * @param keeper the keeper
 * @param indexManager the index manager
 * @param metadataManager the metadata manager
 * @param dataWriter the data writer
 * @param revisionBuilder the revision builder
 */
private[table] class IndexedTableImpl(
    val tableID: QTableID,
    private val keeper: Keeper,
    private val indexManager: IndexManager[DataFrame],
    private val metadataManager: MetadataManager[StructType, FileAction],
    private val dataWriter: DataWriter[DataFrame, StructType, FileAction],
    private val revisionBuilder: RevisionFactory[StructType])
    extends IndexedTable {
  private var snapshotCache: Option[QbeastSnapshot] = None

  override def exists: Boolean = !snapshot.isInitial

  private def checkRevisionParameters(
      qbeastOptions: QbeastOptions,
      latestIndexStatus: IndexStatus): Boolean = {
    // TODO feature: columnsToIndex may change between revisions
    checkColumnsToMatchSchema(latestIndexStatus)
    latestIndexStatus.revision.desiredCubeSize == qbeastOptions.cubeSize

  }

  override def save(
      data: DataFrame,
      parameters: Map[String, String],
      append: Boolean): BaseRelation = {
    val indexStatus =
      if (exists && append) {
        val latestIndexStatus = snapshot.loadLatestIndexStatus
        if (checkRevisionParameters(QbeastOptions(parameters), latestIndexStatus)) {
          latestIndexStatus
        } else {
          val oldRevisionID = latestIndexStatus.revision.revisionID
          val newRevision = revisionBuilder.createNewRevision(tableID, data.schema, parameters)
          IndexStatus(newRevision.copy(revisionID = oldRevisionID + 1))
        }
      } else {
        IndexStatus(revisionBuilder.createNewRevision(tableID, data.schema, parameters))
      }

    val relation = write(data, indexStatus, append)
    relation
  }

  override def load(): BaseRelation = {
    clearCaches()
    createQbeastBaseRelation()
  }

  private def snapshot = {
    if (snapshotCache.isEmpty) {
      snapshotCache = Some(metadataManager.loadSnapshot(tableID))
    }
    snapshotCache.get
  }

  private def clearCaches(): Unit = {
    snapshotCache = None
  }

  private def checkColumnsToMatchSchema(indexStatus: IndexStatus): Unit = {
    val columnsToIndex = indexStatus.revision.columnTransformers.map(_.columnName)
    if (!snapshot.loadLatestRevision.matchColumns(columnsToIndex)) {
      throw AnalysisExceptionFactory.create(
        s"Columns to index '$columnsToIndex' do not match existing index.")
    }
  }

  private def createQbeastBaseRelation(): QbeastBaseRelation = {
    QbeastBaseRelation.forDeltaTable(tableID)
  }

  private def write(data: DataFrame, indexStatus: IndexStatus, append: Boolean): BaseRelation = {
    val revision = indexStatus.revision

    if (exists) {
      keeper.withWrite(indexStatus.revision.tableID, revision.revisionID) { write =>
        val announcedSet = write.announcedCubes
        val updatedStatus = indexStatus.addAnnouncements(announcedSet)
        doWrite(data, updatedStatus, append)
      }
    } else {
      doWrite(data, indexStatus, append)
    }
    clearCaches()
    createQbeastBaseRelation()
  }

  private def doWrite(data: DataFrame, indexStatus: IndexStatus, append: Boolean): Unit = {

    val schema = data.schema
    metadataManager.updateWithTransaction(tableID, schema, append) {
      val (qbeastData, tableChanges) =
        indexManager.index(data, indexStatus)
      val fileActions = dataWriter.write(tableID, schema, qbeastData, tableChanges)
      (tableChanges, fileActions)
    }

  }

  override def analyze(revisionID: RevisionID): Seq[String] = {
    val indexStatus = snapshot.loadIndexStatus(revisionID)
    val cubesToAnnounce = indexManager.analyze(indexStatus)
    keeper.announce(tableID, revisionID, cubesToAnnounce)
    cubesToAnnounce.map(_.string)

  }

  override def optimize(revisionID: RevisionID): Unit = {

    // begin keeper transaction
    val bo = keeper.beginOptimization(tableID, revisionID)

    val currentIndexStatus = snapshot.loadIndexStatus(revisionID)
    val indexStatus = currentIndexStatus.addAnnouncements(bo.cubesToOptimize)
    val cubesToReplicate = indexStatus.cubesToOptimize
    val schema = metadataManager.loadCurrentSchema(tableID)

    if (cubesToReplicate.nonEmpty) {
      metadataManager.updateWithTransaction(tableID, schema, append = true) {
        val dataToReplicate =
          CubeDataLoader(tableID).loadSetWithCubeColumn(
            cubesToReplicate,
            indexStatus.revision,
            QbeastColumns.cubeToReplicateColumnName)
        val (qbeastData, tableChanges) =
          indexManager.optimize(dataToReplicate, indexStatus)
        val fileActions =
          dataWriter.write(tableID, schema, qbeastData, tableChanges)
        (tableChanges, fileActions)
      }
    }

    bo.end(cubesToReplicate)
    // end keeper transaction

    clearCaches()
  }

}
