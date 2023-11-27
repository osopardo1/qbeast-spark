/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.core.model

import io.qbeast.spark.utils.State
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

/**
 * Container for the table changes
 */

object BroadcastedTableChanges {

  def apply(
      revisionChanges: Option[RevisionChange],
      supersededIndexStatus: IndexStatus,
      deltaNormalizedCubeWeights: Map[CubeId, NormalizedWeight],
      deltaCubeDomains: Map[CubeId, Double],
      deltaReplicatedSet: Set[CubeId] = Set.empty,
      deltaAnnouncedSet: Set[CubeId] = Set.empty): TableChanges = {

    val updatedRevision = revisionChanges match {
      case Some(newRev) => newRev.createNewRevision
      case None => supersededIndexStatus.revision
    }
    val cubeWeights = deltaNormalizedCubeWeights
      .mapValues(NormalizedWeight.toWeight)
      .map(identity)

    val replicatedSet = if (revisionChanges.isEmpty) {

      supersededIndexStatus.replicatedSet ++ deltaReplicatedSet

    } else {
      deltaReplicatedSet
    }

    val announcedSet = if (revisionChanges.isEmpty) {

      supersededIndexStatus.announcedSet ++ deltaAnnouncedSet

    } else {
      deltaAnnouncedSet
    }

    val cubeStates = replicatedSet.map(id => id -> State.REPLICATED) ++
      (announcedSet -- replicatedSet).map(id => id -> State.ANNOUNCED)

    BroadcastedTableChanges(
      isNewRevision = revisionChanges.isDefined,
      isOptimizeOperation = deltaReplicatedSet.nonEmpty,
      updatedRevision = updatedRevision,
      deltaReplicatedSet = deltaReplicatedSet,
      announcedOrReplicatedSet = announcedSet ++ replicatedSet,
      cubeStatesBroadcast = SparkSession.active.sparkContext.broadcast(cubeStates.toMap),
      cubeWeightsBroadcast = SparkSession.active.sparkContext.broadcast(cubeWeights),
      cubeDomainsBroadcast = SparkSession.active.sparkContext.broadcast(deltaCubeDomains))
  }

}

case class BroadcastedTableChanges(
    isNewRevision: Boolean,
    isOptimizeOperation: Boolean,
    updatedRevision: Revision,
    deltaReplicatedSet: Set[CubeId],
    announcedOrReplicatedSet: Set[CubeId],
    cubeStatesBroadcast: Broadcast[Map[CubeId, String]],
    cubeWeightsBroadcast: Broadcast[Map[CubeId, Weight]],
    cubeDomainsBroadcast: Broadcast[Map[CubeId, Double]])
    extends TableChanges {

  override def cubeWeight(cubeId: CubeId): Option[Weight] = cubeWeightsBroadcast.value.get(cubeId)

  override def cubeState(cubeId: CubeId): Option[String] = cubeStatesBroadcast.value.get(cubeId)

  override def cubeDomains: Map[CubeId, Double] = cubeDomainsBroadcast.value
}
