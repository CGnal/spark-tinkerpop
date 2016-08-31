package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

/**
 * Utility object exposing methods to conveniently create a `ReplicatedVertexView` instance.
 */
object PersistentReplicatedVertexView {

  /**
   * Creates a `ReplicatedVertexView` instance from edge information.
   * @param edges the edge data
   * @param hasSrcId indicates whether the edge `RDD` has source id information
   * @param hasDstId indicates whether the edge `RDD` has destination id information
   * @tparam A the vertex type
   * @tparam B the edge type
   * @return a `ReplicatedVertexView` instance
   */
  private[graphx] def loadView[A, B](edges: EdgeRDDImpl[B, A],
                                     hasSrcId: Boolean = false,
                                     hasDstId: Boolean = false)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new ReplicatedVertexView[A, B](edges, hasSrcId, hasDstId)

}
