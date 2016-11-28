package org.cgnal.graphe.tinkerpop.titan

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph

import scala.util.{ Try, Success, Failure }

import org.slf4j.LoggerFactory

import org.apache.spark.rdd.RDD

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement
import com.thinkaurelius.titan.core.util.TitanCleanup
import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.util.stats.{ NumberUtil => TitanNumberUtil }

import org.cgnal.graphe.EnrichedRDD
import org.cgnal.graphe.tinkerpop.{ Arrows, TinkerpopEdges }
import org.cgnal.graphe.tinkerpop.graph.{ TinkerTransactionWrapper, TransactionWrapper, NativeTinkerGraphProvider, HadoopGraphLoader }
import org.cgnal.graphe.tinkerpop.titan.hadoop.TitanHbaseInputFormat

/**
 * The Titan-specific implementation of the `TinkerGraphProvider`, providing also native load and save functions.
 * Additionally, this provider also exposes a function that wraps a closure onto a `Management` instance to allow
 * for certain operations as table and schema creation.
 */
object TitanGraphProvider extends NativeTinkerGraphProvider with TitanResourceConfig with HadoopGraphLoader with Serializable {

  @transient lazy val log = LoggerFactory.getLogger("cgnal.titan.Provider")

  @transient protected lazy val graph = TitanFactory.open(config).asInstanceOf[StandardTitanGraph]

  protected def nativeInputFormat = classOf[TitanHbaseInputFormat]

  protected def createTransaction: TransactionWrapper = TinkerTransactionWrapper.create(graph)

  lazy val maxPartitions    = Int.unbox { graph.getConfiguration.getConfiguration.get(GraphDatabaseConfiguration.CLUSTER_MAX_PARTITIONS) }
  lazy val numPartitionBits = TitanNumberUtil getPowerOf2 maxPartitions
  lazy val targetMaxIDs     = 1l << { 63 - numPartitionBits - IDManager.USERVERTEX_PADDING_BITWIDTH }

  private def withIdScaling[A, B, U](rdd: RDD[TinkerpopEdges[A, B]])(f: (Long => TitanId) => U) = {
    log.debug(s"Creating scaler with targetMaxIds [$targetMaxIDs], maxPartitions [$maxPartitions]")
    f { rdd.idLimits.scaled(targetMaxIDs, maxPartitions) }
  }

  /**
   * Saves edges '''and''' vertices.
   */
  private def saveEdges[A, B](rdd: RDD[TinkerpopEdges[A, B]])(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) =
      rdd.foreachPartition {
        TitanTransactionWrapper.batched(graph, _, defaultBatchSize, retryThreshold, retryDelay) { (batch, transaction) =>
          TitanTransactionWrapper.withIdManager(transaction) { idManager =>
            batch.foreach {

              case tinkerpopEdge if tinkerpopEdge.hasNoInEdges => transaction.createVertex(
                tinkerpopEdge.vertex,
                tinkerpopEdge.vertexId.asTitanId.toTitan(idManager)
              ).enrich

              case tinkerpopEdge =>
                val vertex = transaction.createVertex(tinkerpopEdge.vertex, tinkerpopEdge.vertexId.asTitanId.toTitan(idManager)).enriched
                tinkerpopEdge.inEdges.foreach { triplet =>
                  println(triplet)
                  triplet.connect { transaction.createVertex(triplet.srcAttr, triplet.srcId.asTitanId.toTitan(idManager)) -> vertex }
                }
            }
          }
        }.get
    }

  /**
   * Saves vertices '''only'''.
   */
  private def saveVertices[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = rdd.foreachPartition {
    withGraphTransaction(_) { (graph, vertex) => graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*) }.get // let exceptions throw
  }

  /**
   * Opens a `Management` instance and applies `f`, committing the transaction and the end in case of success, and
   * rolling back in case of failure.
   * @param f the function to applu on the fresh `Management` instance
   */
  def withGraphManagement[U](f: TitanManagement => U) = {
    val management = graph.openManagement()
    Try { f(management) } match {
      case Success(u) => management.commit();   graph.tx.close(); u
      case Failure(e) => management.rollback(); graph.tx.close(); throw e
    }
  }

  /**
   * Shuts down and truncates the graph. Note that this is a point of no return: the graph cannot be used beyond
   * this point.
   */
  def clearGraph() = Try {
    log.warn("Shutting down graph instance...")
    graph.close()
    log.warn("Clearing graph...")
    TitanCleanup.clear(graph)
  }

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = saveEdges {
    rdd.filterNot { _.hasNullVertex }
  }

}