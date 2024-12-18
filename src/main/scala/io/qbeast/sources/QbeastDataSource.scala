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
package io.qbeast.sources

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QbeastOptions
import io.qbeast.sources.v2.QbeastTableImpl
import io.qbeast.table.IndexedTableFactory
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.COLUMN_SELECTOR_ENABLED
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.sources.CreatableRelationProvider
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.RelationProvider
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * Qbeast data source is implementation of Spark data source API V1.
 */
class QbeastDataSource private[sources] (private val tableFactory: IndexedTableFactory)
    extends TableProvider
    with CreatableRelationProvider
    with DataSourceRegister
    with RelationProvider
    with Logging {

  /**
   * Constructor to be used by Spark.
   */
  def this() = this(QbeastContext.indexedTableFactory)

  override def shortName(): String = "qbeast"

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = StructType(Seq())

  // Used to get the table of an existing one
  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val tableId = QbeastOptions.loadTableIDFromParameters(properties.asScala.toMap)
    logInfo(s"Getting Qbeast table $tableId")
    val indexedTable = tableFactory.getIndexedTable(tableId)
    if (indexedTable.exists) {
      // If the table exists, we make sure to pass all the properties to QbeastTableImpl
      val currentRevision =
        QbeastContext.metadataManager.loadSnapshot(tableId).loadLatestRevision
      val indexProperties = Map(
        "columnsToIndex" -> currentRevision.columnTransformers.map(_.columnName).mkString(","),
        "cubeSize" -> currentRevision.desiredCubeSize.toString)
      val tableProperties = properties.asScala.toMap ++ indexProperties
      logDebug(s"Table $tableId properties: $tableProperties")
      QbeastTableImpl(
        TableIdentifier(tableId.id),
        new Path(tableId.id),
        tableProperties,
        Some(schema),
        None,
        tableFactory)
    } else {
      QbeastTableImpl(
        TableIdentifier(tableId.id),
        new Path(tableId.id),
        properties.asScala.toMap,
        Some(schema),
        None,
        tableFactory)
    }

  }

  def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = {
    files.head.getPath.getParent
    Some(createRelation(sparkSession.sqlContext, options).schema)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    require(
      parameters.contains("columnsToIndex") || mode == SaveMode.Append || COLUMN_SELECTOR_ENABLED,
      throw AnalysisExceptionFactory.create("'columnsToIndex' is not specified"))

    val tableId = QbeastOptions.loadTableIDFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableId)
    logTrace(s"Begin: Create Qbeast relation $tableId")
    val result = mode match {
      case SaveMode.Append => table.save(data, parameters, append = true)
      case SaveMode.Overwrite => table.save(data, parameters, append = false)
      case SaveMode.ErrorIfExists if table.exists =>
        throw AnalysisExceptionFactory.create(s"The table '$tableId' already exists.")
      case SaveMode.ErrorIfExists => table.save(data, parameters, append = false)
      case SaveMode.Ignore if table.exists => table.load()
      case SaveMode.Ignore => table.save(data, parameters, append = false)
    }
    logTrace(s"End: Create Qbeast relation $tableId")
    result
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val tableID = QbeastOptions.loadTableIDFromParameters(parameters)
    val table = tableFactory.getIndexedTable(tableID)
    logTrace(s"Begin: Create Qbeast relation $tableID")
    if (table.exists) {
      val result = table.load()
      logTrace(s"End: Create Qbeast relation $tableID")
      result
    } else {
      throw AnalysisExceptionFactory.create(
        s"'$tableID' is not a Qbeast formatted data directory.")
    }
  }

}
