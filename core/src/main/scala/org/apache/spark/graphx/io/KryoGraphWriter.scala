package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.graphx.serialization.kryo.KryoRegistry
import org.apache.spark.graphx.{ EdgeTriplet, Graph }
import org.apache.spark.rdd.RDD

/**
 * Writes out a graph into Kryo-serialized data-files.
 * @param graph the graph to be saved
 * @param location the directory where to store the data-files
 * @param registry `KryoRegistry` instance containing all registered serializer classes
 * @tparam A the vertex type
 * @tparam B the edge type
 */
sealed class KryoGraphWriter[A, B](graph: Graph[A, B], location: String, registry: KryoRegistry)(implicit A: ClassTag[A], B: ClassTag[B]) extends GraphWriter[A, B](graph, location) {

  protected def saveVerticesRDD(rdd: => RDD[Vertex[A]]) = KryoGraphIO.writeGrouped[Vertex[A]](registry, s"$location/$vertexLocation") { rdd }

  protected def saveEdgesRDD(rdd: => RDD[PartitionEdge[B, A]]) = KryoGraphIO.writeGrouped[PartitionEdge[B, A]](registry, s"$location/$edgeLocation") { rdd }

  protected def saveTripletsRDD(rdd: => RDD[EdgeTriplet[A, B]]) = KryoGraphIO.writeGrouped[EdgeTriplet[A, B]](registry, s"$location/$tripletLocation") { rdd }

}

object KryoGraphWriter {

  def apply[A, B](graph: Graph[A, B], location: String, registry: KryoRegistry)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new KryoGraphWriter[A, B](graph, location, registry)

}
