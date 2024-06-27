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
package io.qbeast.core.transform

import io.qbeast.core.model.OrderedDataType
import io.qbeast.core.model.QDataType
import io.qbeast.core.model.StringDataType

object HistogramTransformer extends TransformerType {
  override def transformerSimpleName: String = "histogram"

  override def apply(columnName: String, dataType: QDataType): Transformer = dataType match {
    case StringDataType => StringHistogramTransformer(columnName, dataType)
    case _ => HistogramTransformer(columnName, dataType)
  }

  // "a" to "z"
  def defaultStringHistogram: IndexedSeq[String] = (97 to 122).map(_.toChar.toString)

}

case class HistogramTransformer(columnName: String, dataType: QDataType) extends Transformer {

  private val columnHistogram = s"${columnName}_histogram"

  override protected def transformerType: TransformerType = HistogramTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    val names = columnHistogram :: Nil
    val sqlPredicates = s"approx_histogram($columnName) AS $columnHistogram" :: Nil
    ColumnStats(names, sqlPredicates)
  }

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {

    dataType match {
      case ord: OrderedDataType =>
        val hist = row(columnHistogram) match {
          case h: Seq[_] => h.toIndexedSeq
          case _ => ord.defaultHistogram
        }
        HistogramTransformation(hist, ord)
      case _ => throw new IllegalArgumentException(s"Invalid data type: $dataType")
    }

  }

}
