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
import org.apache.spark.sql.AnalysisExceptionFactory

case class CDFNumericQuantilesTransformer(columnName: String, orderedDataType: OrderedDataType)
    extends CDFQuantilesTransformer {

  /**
   * Returns the Transformation given a row representation of the values
   *
   * @param row
   *   the values
   * @return
   *   the transformation
   */
  override def makeTransformation(row: String => Any): Transformation = {
    row(columnTransformerName) match {
      case null => EmptyTransformation()
      case q: Seq[_] if q.nonEmpty =>
        try {
          val quantiles = q.map(_.asInstanceOf[Double]).toIndexedSeq
          CDFNumericQuantilesTransformation(quantiles, orderedDataType)
        } catch {
          case _: ClassCastException =>
            throw AnalysisExceptionFactory.create(
              "Quantiles should be of type Double, but found another type")
        }
      case q: Seq[_] if q.isEmpty =>
        throw AnalysisExceptionFactory.create(
          s"Quantiles for column $columnName size should be greater than 1")
      case _ =>
        throw AnalysisExceptionFactory.create(
          s"Quantiles for column $columnName should be of type Array[Double]")
    }

  }

}
