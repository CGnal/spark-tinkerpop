package org.cgnal.graphe.tinkerpop

import scala.collection.convert.decorateAsJava._

import org.apache.tinkerpop.gremlin.structure.{ Element => TinkerElement, Property => TinkerProperty }

trait TinkerProperties[A <: TinkerElement] { this: TinkerElement =>

  protected def ownerOpt: Option[A]

  protected def rawProperties: Map[String, AnyRef]

  protected lazy val propertiesMap: Map[String, TinkerProperty[AnyRef]] = rawProperties.map { case (key, value) => (key, SparkProperty(key, value, ownerOpt)) }

  private def filterProps[AA](propertyKeys: Seq[String]) = propertyKeys.foldLeft(List.empty[TinkerProperty[AA]]) { (props, key) =>
    propertiesMap.get { key }.map { _.asInstanceOf[TinkerProperty[AA]] :: props } getOrElse props
  }.toIterator

  def properties[AA](propertyKeys: String*) =
    if (propertyKeys.nonEmpty) filterProps[AA](propertyKeys).asJava
    else propertiesMap.valuesIterator.map { _.asInstanceOf[TinkerProperty[AA]] }.asJava

  def property[AA](key: String, value: AA) = propertyAdditionNotSupported

}
