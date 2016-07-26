package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

import org.apache.spark.graphx.{ EdgeTriplet, VertexRDD }
import org.apache.spark.rdd.RDD


final class PersistentGraphImpl[A, B](@transient override val vertices: VertexRDD[A],
                                      @transient override val replicatedVertexView: ReplicatedVertexView[A, B],
                                      @transient protected val tripletsRDD: RDD[EdgeTriplet[A, B]])
                                     (implicit A: ClassTag[A], B: ClassTag[B]) extends GraphImpl[A, B](vertices, replicatedVertexView) {

  protected def this()(implicit A: ClassTag[A], B: ClassTag[B]) = this(null, null, null)

  @transient override lazy val triplets = tripletsRDD

  @transient override val edges = replicatedVertexView.edges

}
