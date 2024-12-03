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
package io.qbeast.catalog

import io.qbeast.core.model.QTableID
import io.qbeast.sources.v2.QbeastTableImpl
import io.qbeast.table.IndexedTable
import io.qbeast.table.IndexedTableFactory
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.TableSpec
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.SparkCatalogV2Util
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.AnalysisExceptionFactory
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.V1TableQbeast

import java.util
import scala.collection.JavaConverters._

/**
 * Object containing all the method utilities for creating and loading a Qbeast formatted Table
 * into the Catalog
 */
object QbeastCatalogUtils extends Logging {

  val QBEAST_ENABLED_CONF: String = "qbeast"

  val supportedProviders = Set("delta")

  /**
   * Checks if the provider is Qbeast
   * @param provider
   *   the provider, if any
   * @return
   */
  def isSupportedProvider(provider: Option[String]): Boolean = {
    provider.isDefined && supportedProviders.contains(provider.get)
  }

  def isQbeastEnabled(properties: Map[String, String]): Boolean = isSupportedProvider(
    properties
      .get("provider")) && properties.getOrElse(QBEAST_ENABLED_CONF, "false").toBoolean

}
