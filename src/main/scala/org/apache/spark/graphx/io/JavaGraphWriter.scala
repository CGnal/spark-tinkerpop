package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.{ Graph, VertexId, PartitionID, EdgeTriplet }
import org.apache.spark.rdd.RDD

sealed class JavaGraphWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) extends GraphWriter[A, B](graph) {

  protected def saveVerticesRDD(rdd: => RDD[(VertexId, A)]) = rdd.saveAsObjectFile { s"$location/$vertexLocation" }

  protected def saveEdgesRDD(rdd: => RDD[(PartitionID, EdgePartition[B, A])]) = rdd.saveAsObjectFile { s"$location/$edgeLocation" }

  protected def saveTripletsRDD(rdd: => RDD[EdgeTriplet[A, B]]) = rdd.saveAsObjectFile { s"$location/$tripletLocation" }

}

object JavaGraphWriter {

  def apply[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new JavaGraphWriter[A, B](graph, location)

}
