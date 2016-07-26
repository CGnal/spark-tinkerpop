package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.impl.EdgeRDDImpl
import org.apache.spark.graphx.{ Graph, EdgeTriplet }

abstract class GraphWriter[A, B](graph: Graph[A, B])(implicit A: ClassTag[A], B: ClassTag[B]) {

  private def extractEdges(graph: Graph[A, B]) = graph.edges.asInstanceOf[EdgeRDDImpl[B, A]].partitionsRDD

  final protected def doNothing: Unit = ()

  protected def saveVerticesRDD(rdd: => RDD[Vertex[A]]): Unit

  protected def saveEdgesRDD(rdd: => RDD[PartitionEdge[B, A]]): Unit

  protected def saveTripletsRDD(rdd: => RDD[EdgeTriplet[A, B]]): Unit

  def saveGraph(): Unit = {
    saveVerticesRDD { graph.vertices      }
    saveEdgesRDD    { extractEdges(graph) }
    saveTripletsRDD { graph.triplets      }
  }

}
