package io.qbeast.model

import io.qbeast.model.Weight.MaxValue

import scala.collection.mutable

/**
 * Builder for creating cube weights.
 *
 * @param desiredSize the desired cube size
 * @param announcedSet the announced cube identifiers
 * @param replicatedSet the replicated cube identifiers
 */
class CubeWeightsBuilder(
    private val desiredSize: Int,
    private val numPartitions: Int,
    announcedSet: Set[CubeId] = Set.empty,
    replicatedSet: Set[CubeId] = Set.empty)
    extends Serializable {
  private val byWeight = Ordering.by[PointWeightAndParent, Weight](_.weight).reverse
  private val queue = new mutable.PriorityQueue[PointWeightAndParent]()(byWeight)

  /**
   * Updates the builder with given point with weight.
   *
   * @param point the point
   * @param weight the weight
   * @param parent the parent cube identifier used to find
   *               the container cube if available
   * @return this instance
   */
  def update(point: Point, weight: Weight, parent: Option[CubeId] = None): CubeWeightsBuilder = {
    queue.enqueue(PointWeightAndParent(point, weight, parent))
    this
  }

  /**
   * Builds the resulting cube weights sequence.
   *
   * @return the resulting cube weights map
   */
  def result(): Seq[CubeNormalizedWeight] = {
    val weights = mutable.Map.empty[CubeId, WeightAndCount]
    while (queue.nonEmpty) {
      val PointWeightAndParent(point, weight, parent) = queue.dequeue()
      val containers = parent match {
        case Some(parentCubeId) => CubeId.containers(point, parentCubeId)
        case None => CubeId.containers(point)
      }
      var continue = true
      while (continue && containers.hasNext) {
        val cubeId = containers.next()
        val weightAndCount = weights.getOrElseUpdate(cubeId, new WeightAndCount(MaxValue, 0))
        if (weightAndCount.count < desiredSize) {
          weightAndCount.count += 1
          if (weightAndCount.count == desiredSize) {
            weightAndCount.weight = weight
          }
          continue = announcedSet.contains(cubeId) || replicatedSet.contains(cubeId)
        }
      }
    }
    weights.map {
      // TODO here we delete the multiplication per numPartition in the first case
      // check if it does not break anything
      case (cubeId, weightAndCount) if weightAndCount.count == desiredSize =>
        val nw = NormalizedWeight(weightAndCount.weight)
        CubeNormalizedWeight(cubeId.bytes, nw)
      case (cubeId, weightAndCount) =>
        CubeNormalizedWeight(
          cubeId.bytes,
          NormalizedWeight(desiredSize, weightAndCount.count) * numPartitions)
    }.toSeq
  }

}

/**
 * Weight and count.
 *
 * @param weight the weight
 * @param count the count
 */
private class WeightAndCount(var weight: Weight, var count: Int)

/**
 * Point, weight and parent cube identifier if available.
 *
 * @param point the point
 * @param weight the weight
 * @param parent the parent
 */
private case class PointWeightAndParent(point: Point, weight: Weight, parent: Option[CubeId])

/**
 * Cube and NormalizedWeight
 *
 * @param cubeBytes the cube
 * @param normalizedWeight the weight
 */
case class CubeNormalizedWeight(cubeBytes: Array[Byte], normalizedWeight: NormalizedWeight)
