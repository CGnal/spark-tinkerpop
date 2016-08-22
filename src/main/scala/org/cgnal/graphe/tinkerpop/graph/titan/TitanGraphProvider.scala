package org.cgnal.graphe.tinkerpop.graph.titan

import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.graphdb.types.system.BaseVertexLabel
import com.thinkaurelius.titan.graphdb.vertices.StandardVertex
import com.thinkaurelius.titan.util.stats.NumberUtil

import scala.annotation.tailrec

import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle
import com.thinkaurelius.titan.graphdb.transaction.{ StandardTitanTx => TitanTransaction }
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

  @tailrec
  final protected def tryTitanCommit(transaction: TitanTransaction, attempt: Int = 1): Unit = Try { transaction.commit() } match {
    case Success(_)                             => log.info(s"Committed transaction at attempt [$attempt] - closing"); transaction.close()
    case Failure(e) if attempt > retryThreshold => transaction.rollback(); transaction.close(); throw new RuntimeException(s"Unable to commit transaction after [$attempt] attemp(s) -- rolling back", e)
    case Failure(_)                             => sleepWarning { s"Failed to commit transaction after attempt [$attempt] -- backing off for [${retryDelay.toSeconds}] second(s)" }; tryTitanCommit(transaction, attempt + 1)
  }

  private def saveEdges[A, B](rdd: RDD[TinkerpopEdges[A, B]])(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = Try {
    rdd.foreachPartition { partition =>
      val transaction   = graph.newTransaction().asInstanceOf[TitanTransaction]
      val partitionBits = NumberUtil.getPowerOf2(2)
      val idManager     = transaction.getIdInspector

      partition.foreach { tinkerpopEdge =>
        tinkerpopEdge.outEdges.foreach { triplet =>
          val inPartition = if (triplet.srcAttr.hashCode() > 0) 1l else 2l
          val inId        = math.abs { triplet.srcAttr.hashCode() }

          val outPartition = if (triplet.dstAttr.hashCode() > 0) 1l else 2l
          val outId        = math.abs { triplet.dstAttr.hashCode() }

          val inV  = new StandardVertex(transaction, idManager.getVertexID(inId,  inPartition,  IDManager.VertexIDType.NormalVertex), ElementLifeCycle.New)
          val outV = new StandardVertex(transaction, idManager.getVertexID(outId, outPartition, IDManager.VertexIDType.NormalVertex), ElementLifeCycle.New)

          inV.addEdge(
            triplet.attr.getClass.getSimpleName,
            outV,
            Arrows.tinkerKeyValuePropSetArrowF(arrowE).apF(triplet.attr): _*
          )

          arrowV.apF(triplet.srcAttr).foldLeft(inV) { (tinkerVertex, prop) =>
            tinkerVertex.property(prop._1, prop._2, Seq.empty[AnyRef]: _*)
            tinkerVertex
          }

        }

        if (tinkerpopEdge.outEdges.isEmpty) {
          val vertexPartition = if (tinkerpopEdge.vertex.hashCode() > 0) 1l else 2l
          val vertexId        = math.abs { tinkerpopEdge.vertex.hashCode() }

          val v = transaction.addVertex(
            idManager.getVertexID(vertexId, vertexPartition, IDManager.VertexIDType.NormalVertex),
            new BaseVertexLabel(tinkerpopEdge.vertexLabelValue))
          arrowV.apF(tinkerpopEdge.vertex).foldLeft(v) { (tinkerVertex, prop) =>
            tinkerVertex.property(prop._1, prop._2, Seq.empty[AnyRef]: _*)
            tinkerVertex
          }
        }
      }
      tryTitanCommit(transaction)
    }
  }

  private def saveVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = Try {
    rdd.foreachPartition {
      withGraphTransaction(_) { (graph, vertex) => graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*) }.get // let exceptions throw
    }
  }

  private def saveAll[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveRawEdges(rdd)

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveAll(rdd, useTinkerpop).get // let exceptions bubble up

}