package org.cgnal.graphe.tinkerpop

import org.apache.spark.graphx.{ EdgeTriplet => SparkEdge }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Vertex => TinkerVertex, T }

case class TinkerpopEdges[A, B](vertexId: Long, vertex: A, inEdges: List[SparkEdge[A, B]], outEdges: List[SparkEdge[A, B]]) {

  def vertexLabel = vertex.getClass.getSimpleName

  def asTinkerVertex(parentGraph: TinkerGraph)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]): TinkerVertex = TinkerSparkVertex(
    vertexId      = vertexId,
    vertexLabel   = vertexLabel,
    parentGraph   = parentGraph,
    inEdges       = inEdges.map  { _.asTinkerEdge(parentGraph) },
    outEdges      = outEdges.map { _.asTinkerEdge(parentGraph) },
    propertiesMap = arrowV.apF(vertex)
  )

  def asVertexKeyValue(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A]): Seq[AnyRef] =
    T.id +: vertexId.toString +: T.label +: vertexLabel +: Arrows.tinkerKeyValuePropSetArrowF(arrowV).apF { vertex }

}

object TinkerpopEdges {

  def apply[A, B](vertexId: Long, vertex: A, allEdges: List[SparkEdge[A, B]]): TinkerpopEdges[A, B] = allEdges.partition { _.dstId == vertexId } match {
    case (inDegrees, outDegrees) => apply(vertexId, vertex, inDegrees, outDegrees)
  }

}
