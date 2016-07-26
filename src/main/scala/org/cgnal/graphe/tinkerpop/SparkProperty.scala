package org.cgnal.graphe.tinkerpop

import org.apache.tinkerpop.gremlin.structure.{ Property => TinkerProperty, VertexProperty => TinkerVertexProperty, Element => TinkerElement, Vertex => TinkerVertex, Graph => TinkerGraph }

case class SparkProperty[A](protected val propKey: String, protected val propValue: A, protected val ownerOpt: Option[TinkerElement] = None) extends TinkerProperty[A] {

  def isPresent = (propValue != null) || propValue.toString.nonEmpty

  def key() = propKey

  def value() = if (isPresent) propValue else propertyMissing { key }

  def remove() = edgeRemovalNotSupported

  def element() = ownerOpt.orNull[TinkerElement]

  def withOwner(element: TinkerElement) = this.copy(ownerOpt = Option { element })

}

case class SparkVertexProperty[A](protected val propId: String,
                                  protected val propKey: String,
                                  protected val propValue: A,
                                  protected val rawProperties: Map[String, AnyRef] = Map.empty[String, AnyRef],
                                  protected val ownerOpt: Option[TinkerVertex] = None) extends TinkerVertexProperty[A] with TinkerProperties[TinkerVertex] {

  def isPresent = (propValue != null) || propValue.toString.nonEmpty

  def key() = propKey

  def value() = if (isPresent) propValue else propertyMissing { key }

  def remove() = edgeRemovalNotSupported

  def element() = ownerOpt.orNull[TinkerVertex]

  def withOwner(vertex: TinkerVertex) = this.copy(ownerOpt = Option { vertex })

  override def id() = propId
}
