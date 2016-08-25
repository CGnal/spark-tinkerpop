package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.{ PersistentReplicatedVertexView, PersistentGraphImpl }
import org.apache.spark.graphx.{ EdgeRDD, Graph, EdgeTriplet, VertexRDD }

abstract class GraphReader[A, B](implicit A: ClassTag[A], B: ClassTag[B]) {

  protected def loadVerticesRDD: RDD[Vertex[A]]

  protected def loadEdgesRDD: RDD[PartitionEdge[B, A]]

  protected def loadTripletsRDD: RDD[EdgeTriplet[A, B]]

  protected  def loadVertexView(hasSrcId: Boolean = false,
                                hasDstId: Boolean = false) = PersistentReplicatedVertexView.loadView[A, B](
      EdgeRDD.fromEdgePartitions[B, A] { loadEdgesRDD },
      hasSrcId,
      hasDstId)

  private def loadVertices   = VertexRDD.apply[A] { loadVerticesRDD }

  def loadGraph(hasSrcId: Boolean = false, hasDstId: Boolean = false): Graph[A, B] = new PersistentGraphImpl[A, B](
    loadVertices,
    loadVertexView(hasSrcId, hasDstId),
    loadTripletsRDD
  )

}

