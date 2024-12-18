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
package org.apache.spark.sources

import io.qbeast.context.QbeastContext
import io.qbeast.core.model.QbeastOptions
import io.qbeast.table.IndexedTableFactory
import org.apache.spark.qbeast.config.COLUMN_SELECTOR_ENABLED
import org.apache.spark.sql.delta.sources.DeltaDataSource
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode

class QbeastDeltaDataSource(private val tableFactory: IndexedTableFactory)
    extends DeltaDataSource {

  /**
   * Constructor to be used by Spark.
   */
  def this() = this(QbeastContext.indexedTableFactory)

  // Just override the createRelation method
  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {

    require(
      parameters.contains("qbeast.enabled") && parameters("qbeast.enabled").toBoolean &&
        (parameters.contains(
          "columnsToIndex") || mode == SaveMode.Append || COLUMN_SELECTOR_ENABLED),
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

}
