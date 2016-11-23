package org.cgnal.graphe.tinkerpop.titan

import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.core.util.{ TitanId => TitanIdUtil }

import org.cgnal.graphe.tinkerpop.TinkerpopEdges

private[titan] case class TitanId(partitionId: Long, id: Long) {

  def toTitan(idManager: IDManager) = idManager.getVertexID(id, partitionId, IDManager.VertexIDType.NormalVertex)

  def toTitanPartitioned(idManager: IDManager) = idManager.getVertexID(id, partitionId, IDManager.VertexIDType.PartitionedVertex)

  def toTitanBasic = TitanIdUtil.toVertexId(id)

}

case class TitanIdLimits(minId: Long, maxId: Long) {

  private lazy val _range = BigDecimal(maxId) - BigDecimal(minId)

  def range = _range

  def ++>(other: TitanIdLimits) = this.copy(
    minId = math.min(this.minId, other.minId),
    maxId = math.max(this.maxId, other.maxId)
  )

  def +>(number: Long) = this.copy(
    minId = math.min(this.minId, number),
    maxId = math.max(this.maxId, number)
  )

  def +>(edges: TinkerpopEdges[_, _]) = this.copy(
    minId = math.min(this.minId, edges.allIds.min),
    maxId = math.max(this.maxId, edges.allIds.max)
  )

  def scaled(number: Long): BigDecimal = (number / range) - (minId / range)

  def scaled(targetMax: Long, targetPartitionMax: Long)(value: Long): TitanId = {
    val scaledId = (scaled(value) * (targetMax - 2)) + 1

    TitanId(
      partitionId = value % targetPartitionMax,
      id          = scaledId.toLong
    )
  }

}

object TitanIdLimits {

  def empty = apply(Long.MaxValue, Long.MinValue)

  def fullRange = apply(Long.MinValue, Long.MaxValue)

}