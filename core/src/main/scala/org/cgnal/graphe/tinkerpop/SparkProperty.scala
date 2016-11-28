package org.cgnal.graphe.tinkerpop

import scala.collection.convert.decorateAsScala._

import org.apache.tinkerpop.gremlin.structure.{ Property => TinkerProperty, VertexProperty => TinkerVertexProperty, Element => TinkerElement, Vertex => TinkerVertex }

case class SparkProperty[A](protected val propKey: String, protected val propValue: A, protected val ownerOpt: Option[TinkerElement] = None) extends TinkerProperty[A] {

  def isPresent = (propValue != null) || propValue.toString.nonEmpty

  def key() = propKey

  def value() = if (isPresent) propValue else propertyMissing { key }

  def remove() = edgeRemovalNotSupported

  def element() = ownerOpt.orNull[TinkerElement]

  def withOwner(element: TinkerElement) = this.copy(ownerOpt = Option { element })

}

object SparkProperty {

  def fromTinkerpop[A](tinkerProperty: TinkerProperty[A]) = apply(
    propKey   = tinkerProperty.key(),
    propValue = tinkerProperty.value()
  )

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

object SparkVertexProperty {

  def fromTinkerpop[A](tinkerVertexProperty: TinkerVertexProperty[A]) = apply(
    propId        = tinkerVertexProperty.id().toString,
    propKey       = tinkerVertexProperty.key(),
    propValue     = tinkerVertexProperty.value(),
    rawProperties = tinkerVertexProperty.properties[AnyRef]().asScala.map { prop => prop.key() -> prop.value() }.toMap
  )

}
