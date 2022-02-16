package io.qbeast.core.transform

import scala.util.Random
import scala.util.hashing.MurmurHash3

/**
 * A hash transformation of a coordinate
 */
case class HashTransformation(nullValue: Any) extends Transformation {

  override def transform(value: Any): Double = {
    val v = if (value == null) nullValue else value
    val hash = v match {
      case s: String =>
        MurmurHash3.bytesHash(s.getBytes)
      case n: Number =>
        MurmurHash3.bytesHash(n.toString.getBytes)
      case a: Array[Byte] =>
        MurmurHash3.bytesHash(a)
    }
    (hash & 0x7fffffff).toDouble / Int.MaxValue
  }

  /**
   * HashTransformation never changes
   * @param newTransformation the new transformation created with statistics over the new data
   *  @return true if the domain of the newTransformation is not fully contained in this one.
   */
  override def isSupersededBy(newTransformation: Transformation): Boolean = false

  override def merge(other: Transformation): Transformation = this
}

object HashTransformation {

  def apply(): HashTransformation = new HashTransformation(Random.nextString(10))

  def apply(nullValue: Any): HashTransformation = new HashTransformation(nullValue)
}