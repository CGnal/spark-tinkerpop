package org.cgnal.graphe.tinkerpop.graph

import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.graphdb.vertices.{StandardVertex, CacheVertex}
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.cgnal.graphe.tinkerpop.{Arrows, TinkerpopEdges}

import scala.util.{ Try, Success, Failure }

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement

import org.cgnal.graphe.tinkerpop.config.ResourceConfig

object TitanGraphProvider extends NativeTinkerGraphProvider with ResourceConfig with Serializable {

  @transient protected val graph = TitanFactory.open(config)

  def withGraphManagement[U](f: TitanManagement => U) = {
    val management = graph.openManagement()
    Try { f(management) } match {
      case Success(u) => management.commit(); graph.tx.close(); u
      case Failure(e) => management.rollback(); graph.tx.close(); throw e
    }
  }

  private def vertexView[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) =
    rdd.mapPartitions { partition =>
      withGraphTransaction { (graph, transaction) =>
        partition.zipWithIndex.flatMap { case (vertex, index) =>
          val titanVertex = graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*)
          if ((index % defaultBatchSize) == 0) { tryCommit(transaction) }
          vertex.outEdges.map { (Long.unbox(titanVertex.id()), _) }
        }
      }.get // let exceptions throw
    }

  private def joinVertices[A, B](rdd: RDD[(Long, EdgeTriplet[A, B])]) = { rdd.keyBy(_._2.dstId) join rdd.keyBy(_._2.srcId) }.map {
    case (_, ((sourceId, sourceTriplet), (destinationId, destinationTriplet))) => (sourceId, destinationId, sourceTriplet.attr)
  }

  private def storeVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = rdd.foreachPartition {
    withGraphTransaction(_) { (graph, vertex) => graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*) }.get // let exceptions throw
  }

  private def storeEdges[A](rdd: RDD[(Long, Long, A)])(implicit arrowE: Arrows.TinkerRawPropSetArrowF[A]) = rdd.foreachPartition { partition =>
    val transaction = graph.newTransaction().asInstanceOf[StandardTitanTx]
    partition.foreach { case (sourceId, destinationId, edge) =>
      new StandardVertex(transaction, sourceId, ElementLifeCycle.Loaded).addEdge(
        edge.getClass.getSimpleName,
        new StandardVertex(transaction, destinationId, ElementLifeCycle.Loaded),
        Arrows.tinkerKeyValuePropSetArrowF(arrowE).apF(edge): _*
      )
    }
    transaction.commit()
  }

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = storeVertices(rdd, useTinkerpop)

}
