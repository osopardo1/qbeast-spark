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

import io.qbeast.core.transform.CDFStringQuantilesTransformer.defaultStringQuantiles

object CDFStringQuantilesTransformer {
  val defaultStringQuantiles: IndexedSeq[String] = (97 to 122).map(_.toChar.toString)
}

case class CDFStringQuantilesTransformer(columnName: String) extends Transformer {
  private val columnHistogram = s"${columnName}_histogram"

  override protected def transformerType: TransformerType = CDFQuantilesTransformer

  /**
   * Returns the stats
   *
   * @return
   */
  override def stats: ColumnStats = {
    ColumnStats(statsNames = columnHistogram :: Nil, statsSqlPredicates = Nil)
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
    val quantiles = row(columnHistogram) match {
      case h: Seq[_] => h.map(_.toString).toIndexedSeq
      case _ => defaultStringQuantiles
    }

    CDFStringQuantilesTransformation(quantiles)
  }

}
