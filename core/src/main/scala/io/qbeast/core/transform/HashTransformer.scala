package io.qbeast.core.transform

import io.qbeast.core.model.QDataType

object HashTransformer extends TransformerType {
  override def transformerSimpleName: String = "hashing"

}

case class HashTransformer(
    columnName: String,
    dataType: QDataType,
    optionalNullValue: Option[Any] = None)
    extends Transformer {
  override protected def transformerType: TransformerType = HashTransformer

  override def stats: ColumnStats = NoColumnStats

  override def maybeMakeTransformation(row: String => Any): Option[Transformation] = {
    optionalNullValue match {
      case Some(nullValue) => Some(HashTransformation(nullValue))
      case None => Some(HashTransformation())
    }
  }

}
