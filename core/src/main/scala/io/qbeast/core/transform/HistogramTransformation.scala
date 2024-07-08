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

import scala.collection.Searching._

case class HistogramTransformation(histogram: IndexedSeq[Any], orderedDataType: OrderedDataType)
    extends Transformation {

  private implicit val ord = orderedDataType.ordering

  private def isDefault: Boolean = histogram == orderedDataType.defaultHistogram

  override def transform(value: Any): Double = {
    histogram.search(value) match {
      case Found(foundIndex) => foundIndex.toDouble / (histogram.length - 1)
      case InsertionPoint(insertionPoint) =>
        if (insertionPoint == 0) 0d
        else if (insertionPoint == histogram.length + 1) 1d
        else (insertionPoint - 1).toDouble / (histogram.length - 1)
      case _ => throw new IllegalArgumentException(s"Value $value not found in histogram")
    }
  }

  /**
   * This method should determine if the new data will cause the creation of a new revision.
   *
   * @param newTransformation
   *   the new transformation created with statistics over the new data
   * @return
   *   true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean =
    newTransformation match {
      case nt @ HistogramTransformation(hist, _) =>
        if (isDefault) !nt.isDefault
        else if (nt.isDefault) false
        else !(histogram == hist)
      case _ => false
    }

  /**
   * Merges two transformations. The domain of the resulting transformation is the union of this
   *
   * @param other
   * @return
   *   a new Transformation that contains both this and other.
   */
  override def merge(other: Transformation): Transformation = other match {
    case _: HistogramTransformation => other
    case _ => this
  }

}
