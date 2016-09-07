package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.graphx.{ Graph, EdgeTriplet }

/**
 * Abstract facade for data saving implementers, providing clients with a simple API to load vertex, edge and triplet
 * information (dependent on implementation).
 * @param graph the graph to be saved
 * @tparam A the vertex type
 * @tparam B the edge type
 */
abstract class GraphWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) {

  private def extractEdges(graph: Graph[A, B]) = graph.edges.asInstanceOf[EdgeRDDImpl[B, A]].partitionsRDD

  /**
   * Saves vertex information.
   * @param rdd the `RDD` containing the vertices
   */
  protected def saveVerticesRDD(rdd: => RDD[Vertex[A]]): Unit

  /**
   * Saves edge information.
   * @param rdd the `RDD` containing the edges
   */
  protected def saveEdgesRDD(rdd: => RDD[PartitionEdge[B, A]]): Unit

  /**
   * Saves edge triplet information
   * @param rdd the `RDD` containing the edge-triplet information { vertex -> edge -> vertex }
   */
  protected def saveTripletsRDD(rdd: => RDD[EdgeTriplet[A, B]]): Unit

  /**
   * Saves a full `Graph[A, B]`, where the default implementation creates a `PersistentGraphImpl[A, B]` utilizing the
   * concrete methods provided by the implementing class.
   */
  def saveGraph(): Unit = {
    saveVerticesRDD { graph.vertices      }
    saveEdgesRDD    { extractEdges(graph) }
    saveTripletsRDD { graph.triplets      }
  }

}
