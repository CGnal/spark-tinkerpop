package org.cgnal.graphe.tinkerpop

import java.util.UUID

import scala.collection.convert.decorateAsScala._

import org.apache.spark.graphx.{ Edge => SparkEdge }
import org.apache.spark.graphx.io.{ Vertex => SparkVertex, vertexId }

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex, Edge => TinkerEdge, VertexProperty => TinkerVertexProperty, Property => TinkerProperty }

/**
 * Arrow abstraction for the forward transformation of `A` to `B`, reference here as `A >-> B`.
 * @tparam A the source type
 * @tparam B the target type
 */
trait >->[A, B] extends Serializable {

  def apF(a: A): B

}

/**
 * Arrow abstraction for the reverse transformation of `B` to `A`, referenced here as `A <-< B`.
 * @tparam A the target type
 * @tparam B the source type
 */
trait <-<[A, B] extends Serializable {

  def apR(b: B): A

}

/**
 * Bi-directional arrow
 * @tparam A the source/target type
 * @tparam B the target/source type
 */
trait >-<[A, B] extends (A >-> B) with (A <-< B)

/**
 * Contains various aliases for common arrow types along with transformations to easy the conversion effort for API
 * users, allowing clients to only need to define a mapping to and from a type `A` to a `Map[String, AnyRef]`.
 */
object Arrows {

  /**
   * Alias for `Map[String, Property[A]]`
   */
  type TinkerPropMap[A]       = Map[String, TinkerProperty[A]]

  /**
   * Alias for `Map[String, VertexProperty[A]]`
   */
  type TinkerVertexPropMap[A] = Map[String, TinkerVertexProperty[A]]

  /**
   * Alias for `A >-> TinkerVertexPropMap[B]`
   */
  type TinkerVertexPropSetArrowF[A, B] = A >-> TinkerVertexPropMap[B]

  /**
   * Alias for `A >-> TinkerPropMap[B]`
   */
  type TinkerPropSetArrowF[A, B]       = A >-> TinkerPropMap[B]

  /**
   * Alias for `A >-> Map[String, AnyRef]`
   */
  type TinkerRawPropSetArrowF[A]       = A >-> Map[String, AnyRef]

  /**
   * Alias for `A <-< TinkerVertexPropMap[B]`
   */
  type TinkerVertexPropSetArrowR[A, B] = A <-< TinkerVertexPropMap[B]

  /**
   * Alias for `A <-< TinkerPropMap[B]`
   */
  type TinkerPropSetArrowR[A, B]       = A <-< TinkerPropMap[B]

  /**
   * Alias for `A <-< Map[String, AnyRef]`
   */
  type TinkerRawPropSetArrowR[A]       = A <-< Map[String, AnyRef]

  /**
   * Alias for `A >-> Vertex`
   */
  type TinkerVertexArrowF[A]           = A >-> TinkerVertex

  /**
   * Alias for `A <-< Vertex`
   */
  type TinkerVertexArrowR[A]           = A <-< TinkerVertex

  /**
   * Alias for `A >-> TinkerEdge`
   */
  type TinkerEdgeArrowF[A]             = A >-> TinkerEdge

  /**
   * Alias for `A <-< TinkerEdge`
   */
  type TinkerEdgeArrowR[A]             = A <-< TinkerEdge

  private def propertyId = UUID.randomUUID().toString.toLowerCase

  private def _refine(props: Map[_, _]) = props.collect {
    case (k, v: AnyRef) => (k.toString, v)
    case (k, v: Int)    => (k.toString, Int.box(v))
    case (k, v: Long)   => (k.toString, Long.box(v))
    case (k, v: Double) => (k.toString, Double.box(v))
    case (k, v: Float)  => (k.toString, Float.box(v))
  }

  private def _asPropMap(key: String, value: Map[_, _]): Map[String, AnyRef] = _refine(value).map {
    case (k, v: Map[_, _]) => key -> _asPropMap(k, v)
    case (k, v)            => key -> v
  }

  private val classArrowF: Class[_] >-> Seq[AnyRef] = new (Class[_] >-> Seq[AnyRef]) {
    override def apF(a: Class[_]): Seq[AnyRef] = Seq (vertexClassKey, a.getCanonicalName)
  }

  /**
   * `Map[String, AnyRef] >-> TinkerVertexPropMap[AnyRef]`
   */
  val mapVertexProperty: TinkerVertexPropSetArrowF[Map[String, AnyRef], AnyRef] = new TinkerVertexPropSetArrowF[Map[String, AnyRef], AnyRef] {
    def apF(props: Map[String, AnyRef]): TinkerVertexPropMap[AnyRef] = props.map {
      case (key, value: Map[_, _]) => key -> SparkVertexProperty[AnyRef](propertyId, key, _asPropMap(key, value))
      case (key, value)            => key -> SparkVertexProperty[AnyRef](propertyId, key, value)
    }
  }

  /**
   * Transforms a `TinkerVertexRawPropSetArrowF[A]` into `A >-> Seq[AnyRef]`, typically used for setting vertex or
   * edge properties.
   */
  implicit def tinkerKeyValuePropSetArrowF[A](implicit arrow: TinkerRawPropSetArrowF[A]): A >-> Seq[AnyRef] = new (A >-> Seq[AnyRef]) {
    def apF(a: A) = arrow.apF(a).flatMap { case (k, v) => Seq(k, v) }.toSeq ++ classArrowF.apF(a.getClass)
  }

  /**
   * Transforms a `TinkerVertexRawPropSetArrowF[A]` into a `A >-> TinkerPropMap[AnyRef]`.
   */
  implicit def tinkerPropSetArrowF[A](implicit arrow: TinkerRawPropSetArrowF[A]): TinkerPropSetArrowF[A, AnyRef] = new TinkerPropSetArrowF[A, AnyRef] {
    def apF(a: A): TinkerPropMap[AnyRef] = arrow.apF(a).map {
      case (key, value: Map[_, _]) => key -> SparkProperty(key, _asPropMap(key, value).asInstanceOf[AnyRef])
      case (key, value)            => key -> SparkProperty[AnyRef](key, value)
    }
  }

  /**
   * Transforms a `TinkerVertexRawPropSetArrowF[A]` into a `A >-> TinkerVertexPropMap[AnyRef]`.
   */
  implicit def tinkerVertexPropSetArrowF[A](implicit arrow: TinkerRawPropSetArrowF[A]): TinkerVertexPropSetArrowF[A, AnyRef] = new TinkerVertexPropSetArrowF[A, AnyRef] {
    def apF(a: A): TinkerVertexPropMap[AnyRef] = arrow.apF(a).map {
      case (key, value: Map[_, _]) => key -> SparkVertexProperty(propertyId, key, _asPropMap(key, value).asInstanceOf[AnyRef])
      case (key, value)            => key -> SparkVertexProperty[AnyRef](propertyId, key, value)
    }
  }

  /**
   * Transforms a `TinkerVertexRawPropSetArrowR[A]` into a `A <-< TinkerVertexPropMap[AnyRef]`.
   */
  implicit def tinkerVertexPropSetArrowR[A](implicit arrow: TinkerRawPropSetArrowR[A]): TinkerPropSetArrowR[A, AnyRef] = new TinkerPropSetArrowR[A, AnyRef] {
    def apR(propSet: TinkerPropMap[AnyRef]) = tinkerPropSetArrowR apR propSet
  }

  /**
   * Transforms a `TinkerVertexRawPropSetArrowR[A]` into a `A <-< TinkerPropMap[AnyRef]`.
   */
  implicit def tinkerPropSetArrowR[A](implicit arrow: TinkerRawPropSetArrowR[A]): TinkerPropSetArrowR[A, AnyRef] = new TinkerPropSetArrowR[A, AnyRef] {
    def apR(propSet: TinkerPropMap[AnyRef]) = arrow.apR {
      propSet.map {
        case (key, prop) if prop.value().isInstanceOf[Map[_, _]] => key -> _asPropMap(prop.key(), prop.value().asInstanceOf[Map[_, _]])
        case (key, prop)                                         => key -> prop.value()
      }
    }
  }

  implicit def tinkerSparkVertexArrowR[A](implicit arrow: TinkerVertexArrowR[A]) = new TinkerVertexArrowR[SparkVertex[A]] {
    def apR(vertex: TinkerVertex) = vertexId { vertex.id() } -> arrow.apR(vertex)
  }

  implicit def tinkerSparkEdgeArrowR[A](implicit arrow: TinkerEdgeArrowR[A]) = new TinkerEdgeArrowR[SparkEdge[A]] {
    def apR(edge: TinkerEdge) = SparkEdge[A] (
      srcId = vertexId { edge.inVertex().id()  },
      dstId = vertexId { edge.outVertex().id() },
      attr  = arrow.apR(edge)
    )
  }

  /**
   * Transforms a `TinkerVertexPropSetArrowR[A, AnyRef]` into a `A <-< Vertex`, which combined with
   * `tinkerVertexPropSetArrowR[A]` chains into `A <-< Map[String, AnyRef] <-< TinkerVertexPropMap[AnyRef] <-< Vertex`
   */
  implicit def tinkerVertexArrowR[A](implicit arrow: TinkerVertexPropSetArrowR[A, AnyRef]) = new TinkerVertexArrowR[A] {
    def apR(vertex: TinkerVertex) = arrow.apR {
      vertex.properties[AnyRef]().asScala.map { prop => prop.key() -> prop }.toMap[String, TinkerVertexProperty[AnyRef]]
    }
  }

  /**
   * Transforms a `TinkerPropSetArrowR[A, AnyRef]` into a `A <-< Edge`, which combined with
   * `tinkerPropSetArrowR[A]` chains into `A <-< Map[String, AnyRef] <-< TinkerPropMap[AnyRef] <-< Edge`
   */
  implicit def tinkerEdgeArrowR[A](implicit arrow: TinkerPropSetArrowR[A, AnyRef]) = new TinkerEdgeArrowR[A] {
    def apR(edge: TinkerEdge) = arrow.apR {
      edge.properties[AnyRef]().asScala.map { prop => prop.key() -> prop }.toMap[String, TinkerProperty[AnyRef]]
    }
  }

}