package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.EdgeTriplet

sealed class JavaGraphReader[A, B](val sparkContext: SparkContext, location: String)(implicit A: ClassTag[A], B: ClassTag[B]) extends GraphReader[A, B] {

  def loadVerticesRDD = sparkContext.objectFile[Vertex[A]] { s"$location/$vertexLocation" }

  def loadEdgesRDD    = sparkContext.objectFile[PartitionEdge[B, A]] { s"$location/$edgeLocation" }

  def loadTripletsRDD = sparkContext.objectFile[EdgeTriplet[A, B]] { s"$location/$tripletLocation" }

}

object JavaGraphReader {

  def apply[A, B](sparkContext: SparkContext, location: String)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new JavaGraphReader[A, B](sparkContext, location)

}

