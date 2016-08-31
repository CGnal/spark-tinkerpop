package org.cgnal.graphe.tinkerpop

import scala.collection.convert.decorateAsJava._

import org.apache.spark.graphx.{ Edge => SparkEdge, EdgeTriplet => SparkEdgeTriplet }

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex, Edge => TinkerEdge, Graph => TinkerGraph, Direction, Element => TinkerElement }

case class TinkerSparkEdge(edgeId: String,
                           edgeLabel: String,
                           parentGraph: TinkerGraph,
                           inVertexOpt: Option[TinkerVertex]  = None,
                           outVertexOpt: Option[TinkerVertex] = None,
                           protected val rawProperties: Map[String, AnyRef] = Map.empty[String, AnyRef]) extends TinkerEdge with TinkerProperties[TinkerElement] {

  protected val ownerOpt = None

  def vertices(direction: Direction) =
    if      (direction == Direction.IN)  inVertexOpt.toIterator.asJava
    else if (direction == Direction.OUT) outVertexOpt.toIterator.asJava
    else { inVertexOpt.toIterator ++ outVertexOpt.toIterator }.asJava

  def remove() = vertexRemovalNotSupported

  def graph() = parentGraph

  def label() = edgeLabel

  def id() = edgeId

}

object TinkerSparkEdge {

  private val magicNumber1 = 6833
  private val magicNumber2 = 32503

  // weak hashing function
  private def generateId(values: Long*) = (values.head + values.tail.reduce { (v1, v2) => (v1 * magicNumber1) + v2 }) * magicNumber2

  def edgeId(sparkEdge: SparkEdge[_]) = generateId(sparkEdge.srcId, sparkEdge.dstId, sparkEdge.attr.hashCode())

  def tripletId(sparkTriplet: SparkEdgeTriplet[_, _]) = generateId(sparkTriplet.srcId, sparkTriplet.dstId, sparkTriplet.attr.hashCode())

}
