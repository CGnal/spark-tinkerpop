package org.cgnal.graphe.tinkerpop.graph

import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD

import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex
import com.thinkaurelius.titan.graphdb.transaction.{ StandardTitanTx => StandardTitanTransaction }
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel

import org.cgnal.graphe.tinkerpop.{Arrows, TinkerpopEdges}

package object titan {

  private type TitanVertexPair[A] = (TitanVertexContainer[A], TitanVertexContainer[A])

  implicit class EnrichedTitanEdges[A, B](rdd: RDD[TinkerpopEdges[A, B]]) {

    def idLimits = rdd.mapPartitions { partition =>
      Iterator {
        partition.foldLeft(TitanIdLimits.empty) { _ +> _.vertexId }
      }
    }.reduce { _ ++> _ }

  }

  implicit class EnrichedTitanTransaction(transaction: StandardTitanTransaction) {

    def standardVertex[A](a: A, titanId: Long) = TitanVertexContainer(
      a,
      new StandardVertex(transaction, titanId, ElementLifeCycle.New)
    )

    def labelVertex[A](a: A, titanId: Long) = TitanVertexContainer(
      a,
      transaction.addVertex(titanId, new BaseVertexLabel(a.getClass.getSimpleName))
    )

  }

  implicit class EnrichedSparkTriplet[A, B](triplet: EdgeTriplet[A, B]) {

    def connect(pair: TitanVertexPair[A])(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]): Unit = {
      pair._1.vertex.addEdge(
        pair._1.a.getClass.getSimpleName,
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
