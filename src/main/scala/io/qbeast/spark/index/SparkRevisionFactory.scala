/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.index

import io.qbeast.core.model.{QDataType, QTableID, Revision, RevisionFactory}
import io.qbeast.spark.internal.QbeastOptions
import io.qbeast.spark.utils.SparkToQTypesUtils
import io.qbeast.core.transform.Transformer
import org.apache.spark.sql.types.StructType

import scala.util.matching.Regex

/**
 * Spark implementation of RevisionBuilder
 */
object SparkRevisionFactory extends RevisionFactory[StructType] {

  // Usage: columnName:transformerType(nullValue)
  val SpecExtractor: Regex = """([^:]+):(.+)""".r
  val TransformerExtractor: Regex = """([A-z]+)\((.+)\)""".r

  def getColumnQType(columnName: String, schema: StructType): QDataType = {
    SparkToQTypesUtils.convertDataTypes(schema(columnName).dataType)
  }

  override def createNewRevision(
      qtableID: QTableID,
      schema: StructType,
      options: Map[String, String]): Revision = {

    val qbeastOptions = QbeastOptions(options)
    val columnSpecs = qbeastOptions.columnsToIndex
    val desiredCubeSize = qbeastOptions.cubeSize
    val transformers = columnSpecs.map {
      case SpecExtractor(columnName, TransformerExtractor(transformerType, nullValue)) =>
        Transformer(transformerType, columnName, nullValue, getColumnQType(columnName, schema))
      case SpecExtractor(columnName, transformerType) =>
        Transformer(transformerType, columnName, getColumnQType(columnName, schema))
      case columnName => Transformer(columnName, getColumnQType(columnName, schema))

    }.toVector

    Revision.firstRevision(qtableID, desiredCubeSize, transformers)
  }

}
