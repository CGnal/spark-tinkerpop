package org.cgnal.graphe.tinkerpop

import org.cgnal.graphe.tinkerpop.graph.EmptyTinkerGraphProvider

import scala.collection.convert.decorateAsJava._
import scala.collection.convert.decorateAsScala._

import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality
import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex, Edge => TinkerEdge, Graph => TinkerGraph, VertexProperty => TinkerProperty, Direction }

case class TinkerSparkVertex(protected val vertexId: Long,
                             protected val vertexLabel: String,
                             protected val parentGraph: TinkerGraph,
                             protected val inEdges: List[TinkerEdge] = List.empty[TinkerEdge],
                             protected val outEdges: List[TinkerEdge] = List.empty[TinkerEdge],
                             protected val propertiesMap: Map[String, TinkerProperty[AnyRef]] = Map.empty[String, TinkerProperty[AnyRef]]) extends TinkerVertex {

  private lazy val ownedPropertiesMap = propertiesMap.map {
    case (k, prop: SparkVertexProperty[_]) => (k, prop withOwner this)
    case other                             => other
  }

  private def filterProperties[A](propertyKeys: Seq[String]) = propertyKeys.foldLeft(List.empty[TinkerProperty[A]]) { (props, key) =>
    ownedPropertiesMap.get { key }.map { _.asInstanceOf[TinkerProperty[A]] :: props } getOrElse props
  }.toIterator.asJava

  private def selectEdges(edgeList: List[TinkerEdge], edgeLabels: Seq[String]) =
    if (edgeLabels.nonEmpty) edgeList.filter { edge => edgeLabels contains edge.label() }.toIterator.asJava
    else edgeList.toIterator.asJava

  override def vertices(direction: Direction, edgeLabels: String*) = { inEdges ::: outEdges }.toIterator.flatMap { edge =>
    if      (direction == Direction.IN  && { edgeLabels contains edge.label() }) Iterator { edge.inVertex()  }
    else if (direction == Direction.OUT && { edgeLabels contains edge.label() }) Iterator { edge.outVertex() }
    else if (edgeLabels contains edge.label()) edge.bothVertices().asScala
    else Iterator.empty
  }.asJava

  def edges(direction: Direction, edgeLabels: String*) =
    if      (direction == Direction.IN)  selectEdges(inEdges, edgeLabels)
    else if (direction == Direction.OUT) selectEdges(outEdges, edgeLabels)
    else selectEdges(inEdges ::: outEdges, edgeLabels)

  def property[A](cardinality: Cardinality, key: String, value: A, keyValues: AnyRef*) = propertyAdditionNotSupported

  def addEdge(label: String, inVertex: TinkerVertex, keyValues: AnyRef*) = edgeAdditionNotSupported

  def properties[A](propertyKeys: String*) =
    if (propertyKeys.nonEmpty) filterProperties[A](propertyKeys)
    else ownedPropertiesMap.valuesIterator.map { _.asInstanceOf[TinkerProperty[A]] }.asJava

  def remove() = propertyRemovalNotSupported

  def graph() = parentGraph

  def label() = vertexLabel

  def id() = vertexId.toString

}

object TinkerSparkVertex {

  def fromTinkerpop(tinkerVertex: TinkerVertex): TinkerSparkVertex = apply(
    vertexId      = tinkerVertex.id().asInstanceOf[Long],
    vertexLabel   = tinkerVertex.label(),
    parentGraph   = EmptyTinkerGraphProvider.emptyGraph,
    inEdges       = tinkerVertex.edges(Direction.IN).asScala.map  { TinkerSparkEdge.fromTinkerpop(_, tinkerVertex.label()) }.toList,
    outEdges      = tinkerVertex.edges(Direction.OUT).asScala.map { TinkerSparkEdge.fromTinkerpop(_, tinkerVertex.label()) }.toList,
    propertiesMap = tinkerVertex.properties[AnyRef]().asScala.map { prop => prop.key() -> SparkVertexProperty.fromTinkerpop(prop) }.toMap
  )

  def fromTinkerpopNeighbor(tinkerVertex: TinkerVertex, label: String): TinkerSparkVertex = apply(
    vertexId      = tinkerVertex.id().asInstanceOf[Long],
    vertexLabel   = label,
    parentGraph   = EmptyTinkerGraphProvider.emptyGraph,
    inEdges       = List.empty,
    outEdges      = List.empty,
    propertiesMap = Map.empty
  )

}
