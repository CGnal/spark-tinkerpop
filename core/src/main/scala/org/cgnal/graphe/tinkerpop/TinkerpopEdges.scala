package org.cgnal.graphe.tinkerpop

import org.apache.spark.graphx.{ EdgeTriplet => SparkEdge }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Vertex => TinkerVertex, T }

case class TinkerpopEdges[A, B](vertexId: Long, vertex: A, inEdges: List[SparkEdge[A, B]], outEdges: List[SparkEdge[A, B]]) {

  def vertexIdKey(useTinkerPopId: Boolean = true): AnyRef = if (useTinkerPopId) T.id else "vertexId"

  def vertexIdValue = vertexId.toString

  def vertexLabelKey(useTinkerPopLabel: Boolean = true): AnyRef = if (useTinkerPopLabel) T.label else "vertexLabel"

  def vertexLabelValue = vertex.getClass.getSimpleName

  def hasNullVertex   = vertex == null

  def hasNullInEdges  = { inEdges contains null }

  def hasNullOutEdges = { outEdges contains null }

  def hasNullEdges    = hasNullInEdges || hasNullOutEdges

  def hasNulls        = hasNullVertex || hasNullEdges

  def allIds = vertexId +: { inEdges.map { _.srcId } ++ outEdges.map { _.dstId } }

  def allEdges = inEdges ::: outEdges

  def hasNoInEdges  = inEdges.isEmpty

  def hasNoOutEdges = outEdges.isEmpty

  def isIsolated    = hasNoInEdges && hasNoOutEdges

  def isNotIsolated = !isIsolated

  def asTinkerVertex(parentGraph: TinkerGraph)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]): TinkerVertex = TinkerSparkVertex(
    vertexId      = vertexId,
    vertexLabel   = vertexLabelValue,
    parentGraph   = parentGraph,
    inEdges       = inEdges.map  { _.asTinkerEdge(parentGraph) }.toArray,
    outEdges      = outEdges.map { _.asTinkerEdge(parentGraph) }.toArray,
    propertiesMap = arrowV.apF(vertex)
  )

  def asVertexKeyValue(useTinkerpopKeys: Boolean = true)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A]): Seq[AnyRef] =
    vertexIdKey(useTinkerpopKeys)    +: vertexIdValue    +:
    vertexLabelKey(useTinkerpopKeys) +: vertexLabelValue +:
    Arrows.tinkerKeyValuePropSetArrowF(arrowV).apF { vertex }

}

object TinkerpopEdges {

  def apply[A, B](vertexId: Long, vertex: A, allEdges: List[SparkEdge[A, B]]): TinkerpopEdges[A, B] = allEdges.partition { _.dstId == vertexId } match {
    case (inDegrees, outDegrees) => apply(vertexId, vertex, inDegrees, outDegrees)
  }

  def empty[A, B](vertexId: Long, vertex: A): TinkerpopEdges[A, B] = apply[A, B](vertexId, vertex, List.empty[SparkEdge[A, B]], List.empty[SparkEdge[A, B]])

}
