package org.cgnal.graphe.tinkerpop.graph.titan

import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx
import com.thinkaurelius.titan.graphdb.types.vertices.EdgeLabelVertex
import org.apache.spark.graphx.EdgeTriplet
import org.apache.spark.rdd.RDD
import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph }
import org.cgnal.graphe.tinkerpop.{Arrows, TinkerpopEdges}

import scala.util.{ Try, Success, Failure }

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement

import org.cgnal.graphe.tinkerpop.config.ResourceConfig
import org.cgnal.graphe.tinkerpop.graph.NativeTinkerGraphProvider

object TitanGraphProvider extends NativeTinkerGraphProvider with ResourceConfig with Serializable {

  @transient protected val graph = TitanFactory.open(config)

  def withGraphManagement[U](f: TitanManagement => U) = {
    val management = graph.openManagement()
    Try { f(management) } match {
      case Success(u) => management.commit(); graph.tx.close(); u
      case Failure(e) => management.rollback(); graph.tx.close(); throw e
    }
  }

  private def findIn[A, B](rdd: RDD[TinkerpopEdges[A, B]]) = Try {
    rdd.flatMap { _.inEdges }
  }

  private def findOut[A, B](rdd: RDD[TinkerpopEdges[A, B]]) = Try {
    rdd.flatMap { _.outEdges }
  }

  private def joinIn[A, B](idRDD: RDD[TitanId], edges: RDD[EdgeTriplet[A, B]]) = Try {
    { idRDD.keyBy(_.sparkId) join edges.keyBy(_.dstId) }.map {
      case (_, triplet @ (titanId, outTriplet)) => (outTriplet.srcId, outTriplet.dstId) -> triplet
    }
  }

  /*            /-> V -> E -> V
   *       /-> E     /-> E /
   *      V -> E -> V -> E -> V
   *       \-> E
   *            \-> V -> E -> V
   */
  private def joinOut[A, B](idRDD: RDD[TitanId], edges: RDD[EdgeTriplet[A, B]]) = Try {
    { idRDD.keyBy(_.sparkId) join edges.keyBy(_.srcId) }.map {
      case (_, triplet @ (titanId, outTriplet)) => (outTriplet.srcId, outTriplet.dstId) -> triplet
    }
  }

  private def joinAll[A, B](inRDD: RDD[EdgeKey[(TitanId, EdgeTriplet[A, B])]], outRDD: RDD[EdgeKey[(TitanId, EdgeTriplet[A, B])]]) = Try {
    { inRDD join outRDD }.map {
      case (_, ((sourceId, _), (destinationId, triplet))) => (sourceId.titanId, destinationId.titanId) -> triplet.attr
    }
  }

  private def saveEdges[A](rdd: RDD[EdgeKey[A]])(implicit arrow: Arrows.TinkerRawPropSetArrowF[A]) = Try {
    rdd.foreachPartition { partition =>
      val transaction = graph.newTransaction().asInstanceOf[StandardTitanTx]
      partition.foreach { case ((inId, outId), attr) =>
        new EdgeLabelVertex(transaction, inId, ElementLifeCycle.Loaded).addEdge(
          attr.getClass.getSimpleName,
          new EdgeLabelVertex(transaction, outId, ElementLifeCycle.Loaded),
          Arrows.tinkerKeyValuePropSetArrowF(arrow).apF(attr): _*
        )
      }
      transaction.commit()
    }
  }

  private def saveVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = Try {
    rdd.foreachPartition {
      withGraphTransaction(_) { (graph, vertex) => graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*) }.get // let exceptions throw
    }
  }

  private def createVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = Try {
    rdd.mapPartitions { partition =>
      withGraphTransaction { graph: TinkerGraph =>
        partition.map { vertex => TitanId(vertex.vertexId, graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*)) }
      }.get // let exception bubble up
    }
  }

  private def saveAll[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = for {
    vertices <- createVertices(rdd, useTinkerpop)
    ins      <- findIn  { rdd }
    inJoin   <- joinIn(vertices, ins)
    outs     <- findOut { rdd }
    outJoin  <- joinOut(vertices, outs)
    allJoin  <- joinAll(inJoin, outJoin)
    _        <- saveEdges(allJoin)
  } yield ()

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveAll(rdd, useTinkerpop).get // let exceptions bubble up

}
