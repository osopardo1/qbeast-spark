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

object CDFQuantilesTransformer extends TransformerType {
  override def transformerSimpleName: String = "quantiles"

  override def apply(columnName: String, dataType: QDataType): Transformer = {
    dataType match {
      case ord: OrderedDataType => CDFNumericQuantilesTransformer(columnName, ord)
      case StringDataType => CDFStringQuantilesTransformer(columnName)
      case _ =>
        throw new IllegalArgumentException(
          s"CDFQuantilesTransformer can only be applied to OrderedDataType columns or StringDataType columns. " +
            s"Column $columnName is of type $dataType")
    }
  }

}
