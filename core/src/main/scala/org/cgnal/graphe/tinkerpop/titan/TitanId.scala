package org.cgnal.graphe.tinkerpop.titan

import com.thinkaurelius.titan.graphdb.idmanagement.IDManager

private[titan] case class TitanId(partitionId: Long, id: Long) {

  def toTitan(idManager: IDManager) = idManager.getVertexID(id, partitionId, IDManager.VertexIDType.NormalVertex)

}

private[titan] case class TitanIdLimits(minId: Long, maxId: Long) {

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

  def scaled(number: Long): BigDecimal = (number / range) - (minId / range)

  def scaled(targetMax: Long, targetPartitionMax: Long)(value: Long): TitanId = {
    val scaledId = (scaled(value) * (targetMax - 2)) + 1
    TitanId(
      partitionId = { scaledId % targetPartitionMax }.toLong,
      id          = scaledId.toLong
    )
  }

}

object TitanIdLimits {

  def empty = apply(Long.MaxValue, Long.MinValue)

  def fullRange = apply(Long.MinValue, Long.MaxValue)

}