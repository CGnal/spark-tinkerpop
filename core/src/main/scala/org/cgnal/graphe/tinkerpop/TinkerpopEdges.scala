package org.cgnal.graphe.tinkerpop

import org.apache.spark.graphx.{ EdgeTriplet => SparkEdge }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Vertex => TinkerVertex, T }

case class TinkerpopEdges[A, B](vertexId: Long, vertex: A, inEdges: List[SparkEdge[A, B]], outEdges: List[SparkEdge[A, B]]) {

  def vertexIdKey(useTinkerPop: Boolean = true): AnyRef = if (useTinkerPop) T.id else "vertexId"

  def vertexIdValue = vertexId.toString

  def vertexLabelKey(useTinkerPop: Boolean = true): AnyRef = if (useTinkerPop) T.label else "vertexLabel"

  def vertexLabelValue = vertex.getClass.getSimpleName

  def hasNullVertex   = vertex == null

  def hasNullInEdges  = { inEdges contains null }

  def hasNullOutEdges = { outEdges contains null }

  def hasNullEdges    = hasNullInEdges || hasNullOutEdges

  def hasNulls        = hasNullVertex || hasNullEdges

  def asTinkerVertex(parentGraph: TinkerGraph, useTinkerpop: Boolean = true)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]): TinkerVertex = TinkerSparkVertex(
    vertexId      = vertexId,
    vertexLabel   = vertexLabelValue,
    parentGraph   = parentGraph,
    inEdges       = inEdges.map  { _.asTinkerEdge(parentGraph) },
    outEdges      = outEdges.map { _.asTinkerEdge(parentGraph) },
    propertiesMap = arrowV.apF(vertex)
  )

  def asVertexKeyValue(useTinkerpop: Boolean = true)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A]): Seq[AnyRef] =
    vertexIdKey(useTinkerpop)    +: vertexIdValue    +:
    vertexLabelKey(useTinkerpop) +: vertexLabelValue +:
    Arrows.tinkerKeyValuePropSetArrowF(arrowV).apF { vertex }

}

object TinkerpopEdges {

  def apply[A, B](vertexId: Long, vertex: A, allEdges: List[SparkEdge[A, B]]): TinkerpopEdges[A, B] = allEdges.partition { _.dstId == vertexId } match {
    case (inDegrees, outDegrees) => apply(vertexId, vertex, inDegrees, outDegrees)
  }

}
