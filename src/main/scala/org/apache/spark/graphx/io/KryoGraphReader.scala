package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.serialization.kryo.KryoRegistry
import org.apache.spark.graphx.EdgeTriplet

/**
 * Reads Kryo-serialized data into a `Graph[A, B]`.
 * @param sparkContext the `SparkContext` instance into which to load the resulting `RDD`
 * @param location the path to the directory where to load the data from
 * @param registry `KryoRegistry` instance containing all registered serializer classes
 * @tparam A the vertex type
 * @tparam B the edge type
 */
sealed class KryoGraphReader[A, B](sparkContext: SparkContext, location: String, registry: KryoRegistry)(implicit A: ClassTag[A], B: ClassTag[B]) extends GraphReader[A, B] {

  protected def loadVerticesRDD = KryoGraphIO.readGrouped[Vertex[A]](sparkContext, registry) { s"$location/$vertexLocation" }

  protected def loadEdgesRDD    = KryoGraphIO.readGrouped[PartitionEdge[B, A]](sparkContext, registry) { s"$location/$edgeLocation" }

  protected def loadTripletsRDD = KryoGraphIO.readGrouped[EdgeTriplet[A, B]](sparkContext, registry) { s"$location/$tripletLocation" }

}

object KryoGraphReader extends Serializable {

  def apply[A, B](sparkContext: SparkContext, location: String, registry: KryoRegistry)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new KryoGraphReader[A, B](sparkContext, location, registry)

}
