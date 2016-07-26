package org.cgnal.graphe.tinkerpop.graph

import java.util.Optional
import java.util.{ Iterator => JavaIterator }

import scala.collection.convert.decorateAsJava._

import org.apache.commons.configuration.Configuration

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer
import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Vertex => TinkerVertex, Edge => TinkerEdge }

import org.cgnal.graphe.tinkerpop._

final class EmptyTinkerGraph(@transient private val innerConfiguration: Configuration) extends TinkerGraph with Serializable {

  def vertices(vertexIds: AnyRef*): JavaIterator[TinkerVertex] = Seq.empty[TinkerVertex].toIterator.asJava

  def tx() = new EmptyTransaction(this)

  def edges(edgeIds: AnyRef*): JavaIterator[TinkerEdge] = Seq.empty[TinkerEdge].toIterator.asJava

  def variables() = new EmptyTinkerVariables

  def configuration() = innerConfiguration

  def addVertex(keyValues: AnyRef*) = vertexAdditionNotSupported

  def compute[C <: GraphComputer](graphComputerClass: Class[C]) = computationNotSupported

  def compute() = computationNotSupported

  def close() = ()

  override def features() = EmptyGraphFeatures

}

private[tinkerpop] object EmptyGraphFeatures extends TinkerGraph.Features {

  override def vertex() = EmptyGraphVertexFeatures

}

private[tinkerpop] object EmptyGraphVertexFeatures extends TinkerGraph.Features.VertexFeatures {

  override def supportsAddVertices()    = false

  override def supportsRemoveVertices() = false

  override def supportsMetaProperties() = true

}

sealed class EmptyTinkerVariables extends TinkerGraph.Variables {

  def set(key: String, value: scala.Any) = ()

  def get[R](key: String) = Optional.empty[R]

  def remove(key: String) = ()

  def keys() = Set.empty[String].asJava

}
