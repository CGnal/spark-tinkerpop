package org.cgnal.graphe.tinkerpop.graph.titan

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

case class TitanId(sparkId: Long, titanId: Long)

object TitanId {

  def apply(sparkId: Long, titanId: AnyRef): TitanId = apply(
    sparkId,
    Long.unbox { titanId }
  )

  def apply(sparkId: Long, titanVertex: TinkerVertex): TitanId = apply(
    sparkId,
    titanVertex.id()
  )

}