package org.cgnal.graphe.tinkerpop

import scala.reflect.ClassTag

import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

import com.thinkaurelius.titan.core.TitanVertex
import com.thinkaurelius.titan.core.schema.{ VertexLabelMaker => TitanVertexLabelMaker, EdgeLabelMaker => TitanEdgeLabelMaker, PropertyKeyMaker => TitanPropertyKeyMaker, TitanManagement }
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex
import com.thinkaurelius.titan.graphdb.transaction.{ StandardTitanTx => StandardTitanTransaction }

/**
 * Package object containing implicit enrichment classes to provide convenient suffix methods.
 */
package object titan {

  private type TitanVertexPair[A] = (TitanVertexContainer[A], TitanVertexContainer[A])

  implicit class EnrichedId(id: Long) {

    def asTitanId: TitanId = TitanId(1l, id + 1)

    def asTitanId(numPartitions: Long) = TitanId(id % numPartitions, id + 1)

  }

  /**
   * Enriches a Titan `Management` instance to allow less fussy schema creation functions, providing a closure around
   * the pertinent builder, leveraging their fluent APIs.
   */
  implicit class EnrichedTitanManagement(m: TitanManagement) {

    def createVertexLabel(name: String)(f: TitanVertexLabelMaker => TitanVertexLabelMaker): Unit =
      if (m.getVertexLabel(name) == null) f { m.makeVertexLabel(name) }.make()

    def createEdgeLabel(name: String)(f: TitanEdgeLabelMaker => TitanEdgeLabelMaker): Unit =
      if (m.getEdgeLabel(name) == null) f { m.makeEdgeLabel(name) }.make()

    def createPropertyKey[A](name: String)(f: TitanPropertyKeyMaker => TitanPropertyKeyMaker)(implicit A: ClassTag[A]): Unit =
      if (m.getPropertyKey(name) == null) f { m.makePropertyKey(name).dataType(A.runtimeClass.asInstanceOf[Class[A]]) }.make()

  }

  /**
   * Enriches an `RDD[TinkerpopEdges[A, B]]` to add a function that collects the global limits of the vertex ids,
   * conveniently returned in an instance of `TitanIdLimits`.
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  implicit class EnrichedTitanEdges[A, B](rdd: RDD[TinkerpopEdges[A, B]]) {

    def idLimits = rdd.mapPartitions { partition =>
      Iterator {
        partition.foldLeft(TitanIdLimits.empty) { _ +> _ }
      }
    }.reduce { _ ++> _ }

  }

  /**
   * Enriches a `StandardTitanTx` instance to allow creation of a `TitanVertexContainer`, which wraps a created
   * `StandardVertex` or tinkerpop `Vertex` to allow further enrichment by adding properties. This is simply an optional
   * but more fluent way of creating a vertex and assigning properties to it.
   */
  implicit class EnrichedTitanTransaction(transaction: StandardTitanTransaction) {

    def getOrCreateVertex(titanId: Long)(default: => TitanVertex) = Option { transaction.getVertex(titanId) } getOrElse default

    /**
     * Creates a `StandardVertex` wrapped in a `TitanVertexContainer`
     * @param a the original vertex  class that needs to be converted into a Titan vertex
     * @param titanId the desired id as created using an `IDManager` instance
     * @tparam A the vertex type
     */
    def standardVertex[A](a: A, titanId: Long) = TitanVertexContainer(
      a,
      getOrCreateVertex(titanId) { new StandardVertex(transaction, titanId, ElementLifeCycle.Loaded) }
    )

    /**
     * Creates a tinkerpop `Vertex` wrapped in a `TitanVertexContainer`
     * @param a the original vertex  class that needs to be converted into a Titan vertex
     * @param titanId the desired id as created using an `IDManager` instance
     * @tparam A the vertex type
     */
    def createVertex[A](a: A, titanId: Long) = TitanVertexContainer(
      a,
      getOrCreateVertex(titanId) { transaction.addVertex(Long.box(titanId), transaction.getVertexLabel(a.getClass.getSimpleName)) }
    )

  }

  /**
   * Enriches a Spark `EdgeTriplet` with a suffix method that allows to connect a `TitanVertexPair[A]` with an edge,
   * also implicitly performing the conversion using the supplied `Arrow`s. The intended syntax here is
   * {{{
   *   triplet.connect { container1 -> container2 }
   * }}}
   * which connects a vertex container `container1` to another one `container2` in that direction.
   * @param triplet the enriched triplet instance
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  implicit class EnrichedSparkTriplet[A, B](triplet: EdgeTriplet[A, B]) {

    def connect(pair: TitanVertexPair[A])(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]): Unit = {
      pair._1.vertex.addEdge(
        triplet.attr.getClass.getSimpleName,
        pair._2.vertex,
        Arrows.tinkerKeyValuePropSetArrowF(arrowE).apF(triplet.attr): _*
      )

      arrowV.apF(triplet.srcAttr).foldLeft(pair._1.vertex) { (vertex, prop) =>
        vertex.property(prop._1, prop._2, Seq.empty[AnyRef]: _*)
        vertex
      }
    }

  }

}
