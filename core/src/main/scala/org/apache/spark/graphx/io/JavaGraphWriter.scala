package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.{ Graph, VertexId, PartitionID, EdgeTriplet }
import org.apache.spark.rdd.RDD

/**
 * Direct implementation of `GraphWriter`, saving graph data on a file-system (typically HDFS) in Java-serialized binary
 * form. Note that this class is sealed and all instantiation methods are delegated to the companion object.
 * @param graph the graph to store
 * @param location the path to the directory where to store the binary data
 * @param A the vertex type
 * @param B the edge type
 */
sealed class JavaGraphWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) extends GraphWriter[A, B](graph, location) {

  protected def saveVerticesRDD(rdd: => RDD[(VertexId, A)]) = rdd.saveAsObjectFile { s"$location/$vertexLocation" }

  protected def saveEdgesRDD(rdd: => RDD[(PartitionID, EdgePartition[B, A])]) = rdd.saveAsObjectFile { s"$location/$edgeLocation" }

  protected def saveTripletsRDD(rdd: => RDD[EdgeTriplet[A, B]]) = rdd.saveAsObjectFile { s"$location/$tripletLocation" }

}

object JavaGraphWriter {

  def apply[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new JavaGraphWriter[A, B](graph, location)

}
