package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.{ PersistentReplicatedVertexView, PersistentGraphImpl }
import org.apache.spark.graphx.{ EdgeRDD, Graph, EdgeTriplet, VertexRDD }

/**
 * Abstract facade for data loading implementers, providing clients with a simple API to load vertex, edge and triplet
 * information (dependent on implementation).
 * @tparam A the vertex type.
 * @tparam B the edge type.
 */
abstract class GraphReader[A, B](implicit A: ClassTag[A], B: ClassTag[B]) {

  /**
   * Loads the vertex data into an `RDD`.
   */
  protected def loadVerticesRDD: RDD[Vertex[A]]

  /**
   * Loads the edge data into an `RDD`.
   */
  protected def loadEdgesRDD: RDD[PartitionEdge[B, A]]

  /**
   * Loads triplet data into an `RDD`.
   */
  protected def loadTripletsRDD: RDD[EdgeTriplet[A, B]]

  /**
   * Loads a `ReplicatedVertexView`, where the default implementation utilizes the implementer's `loadEdgesRDD`.
   * @param hasSrcId indicates whether the edge information also contains the source id
   * @param hasDstId indicates whether the edge information also contains the destination id
   */
  protected  def loadVertexView(hasSrcId: Boolean = false,
                                hasDstId: Boolean = false) = PersistentReplicatedVertexView.loadView[A, B](
      EdgeRDD.fromEdgePartitions[B, A] { loadEdgesRDD },
      hasSrcId,
      hasDstId)

  private def loadVertices   = VertexRDD.apply[A] { loadVerticesRDD }

  /**
   * Loads a full `Graph[A, B]`, where the default implementation creates a `PersistentGraphImpl[A, B]` utilizing the
   * concrete methods provided by the implementing class.
   * @param hasSrcId indicates whether the edge information also contains source id
   * @param hasDstId indicates whether the edge information also contains the destination id
   */
  def loadGraph(hasSrcId: Boolean = false, hasDstId: Boolean = false): Graph[A, B] = new PersistentGraphImpl[A, B](
    loadVertices,
    loadVertexView(hasSrcId, hasDstId),
    loadTripletsRDD
  )

}

