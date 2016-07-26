package org.apache.spark.graphx.impl

import scala.reflect.ClassTag

object PersistentReplicatedVertexView {

  private[graphx] def loadView[A, B](edges: EdgeRDDImpl[B, A],
                                     hasSrcId: Boolean = false,
                                     hasDstId: Boolean = false)(implicit A: ClassTag[A], B: ClassTag[B]) =
    new ReplicatedVertexView[A, B](edges, hasSrcId, hasDstId)

}
