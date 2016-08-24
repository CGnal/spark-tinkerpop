package org.cgnal.graphe.tinkerpop.graph.titan

import scala.util.{ Try, Success, Failure }

import org.apache.spark.rdd.RDD

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.util.stats.{ NumberUtil => TitanNumberUtil }

import org.cgnal.graphe.tinkerpop.{ Arrows, TinkerpopEdges }
import org.cgnal.graphe.tinkerpop.config.ResourceConfig
import org.cgnal.graphe.tinkerpop.hadoop.TitanHbaseInputFormat
import org.cgnal.graphe.tinkerpop.graph.{ TinkerTransactionWrapper, TransactionWrapper, NativeTinkerGraphProvider }

object TitanGraphProvider extends NativeTinkerGraphProvider with ResourceConfig with Serializable {

  @transient protected val graph = TitanFactory.open(config)

  protected def nativeInputFormat = classOf[TitanHbaseInputFormat]

  protected def createTransaction: TransactionWrapper = TinkerTransactionWrapper.create(graph)

  private def maxPartitions    = config.getInt("cluster.max-partitions", 32)
  private def numPartitionBits = TitanNumberUtil getPowerOf2 maxPartitions
  private def targetMaxIDs     = 1l << (63l - numPartitionBits - IDManager.USERVERTEX_PADDING_BITWIDTH);

  private def withIdScaling[A, B, U](rdd: RDD[TinkerpopEdges[A, B]])(f: (Long => TitanId) => U) = {
    f { TitanIdLimits.fullRange.scaled(targetMaxIDs, maxPartitions) }
  }

  private def saveEdges[A, B](rdd: RDD[TinkerpopEdges[A, B]])(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) =
    withIdScaling(rdd) { scaler =>
      rdd.foreachPartition { partition =>
       TitanTransactionWrapper.standard(graph).attemptTitanTransaction(retryThreshold, retryDelay) {
        TitanTransactionWrapper.withIdManager(_) { (idManager, transaction) =>

          partition.foreach {
            case tinkerpopEdge if tinkerpopEdge.outEdges.isEmpty => transaction.labelVertex(
              tinkerpopEdge.vertex,
              scaler(tinkerpopEdge.vertexId).toTitan(idManager)
            ).enrich
            case tinkerpopEdge                                   => tinkerpopEdge.outEdges.foreach { triplet =>
              triplet.connect {
                transaction.standardVertex(triplet.srcAttr, scaler(triplet.srcId).toTitan(idManager)) ->
                transaction.standardVertex(triplet.dstAttr, scaler(triplet.dstId).toTitan(idManager))
              }
            }
          }
        }
      }.get
    }
  }

  private def saveVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = rdd.foreachPartition {
    withGraphTransaction(_) { (graph, vertex) => graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*) }.get // let exceptions throw
  }

  private def saveAll[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveEdges(rdd)

  def withGraphManagement[U](f: TitanManagement => U) = {
    val management = graph.openManagement()
    Try { f(management) } match {
      case Success(u) => management.commit();   graph.tx.close(); u
      case Failure(e) => management.rollback(); graph.tx.close(); throw e
    }
  }

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveAll(rdd, useTinkerpop)

}