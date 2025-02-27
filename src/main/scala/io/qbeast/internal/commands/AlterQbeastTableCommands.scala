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
package io.qbeast.internal.commands

import io.qbeast.sources.v2.QbeastTableImpl
import org.apache.spark.internal.Logging
import org.apache.spark.qbeast.config.DEFAULT_TABLE_FORMAT
import org.apache.spark.sql.catalyst.plans.logical.IgnoreCachedData
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.commands.AlterTableUnsetPropertiesDeltaCommand
import org.apache.spark.sql.delta.DeltaConfigs
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

case class AlterTableSetPropertiesQbeastCommand(
    qbeastTable: QbeastTableImpl,
    configuration: Map[String, String])
    extends LeafRunnableCommand
    with Logging {

  override def run(spark: SparkSession): Seq[Row] = {
    log.info(s"Setting properties $configuration for table ${qbeastTable.tableIdentifier}")

    DEFAULT_TABLE_FORMAT match {
      case "delta" =>
        val deltaTable =
          DeltaTableV2(
            spark,
            qbeastTable.path,
            qbeastTable.catalogTable,
            Some(qbeastTable.tableIdentifier.toString),
            None,
            qbeastTable.options)
        AlterTableSetPropertiesDeltaCommand(
          deltaTable,
          DeltaConfigs.validateConfigurations(configuration))
          .run(spark)

      case _ =>
        throw new IllegalArgumentException(
          s"AlterTableCommand for table format $DEFAULT_TABLE_FORMAT not found")
    }

  }

}

case class AlterTableUnsetPropertiesQbeastCommand(
    qbeastTable: QbeastTableImpl,
    propKeys: Seq[String],
    ifExists: Boolean)
    extends LeafRunnableCommand
    with IgnoreCachedData
    with Logging {

  override def run(spark: SparkSession): Seq[Row] = {
    log.info(s"Unsetting properties $propKeys for table ${qbeastTable.tableIdentifier}")
    DEFAULT_TABLE_FORMAT match {
      case "delta" =>
        val deltaTable =
          DeltaTableV2(
            spark,
            qbeastTable.path,
            qbeastTable.catalogTable,
            Some(qbeastTable.tableIdentifier.toString),
            None,
            qbeastTable.options)
        AlterTableUnsetPropertiesDeltaCommand(
          deltaTable,
          propKeys,
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case _ =>
        throw new IllegalArgumentException(
          s"AlterTableCommand for table format $DEFAULT_TABLE_FORMAT not found")
    }
  }

}
