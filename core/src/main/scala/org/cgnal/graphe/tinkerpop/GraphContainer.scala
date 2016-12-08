package org.cgnal.graphe.tinkerpop

import scala.reflect.ClassTag

import org.apache.spark.graphx.{ Graph => SparkGraph }
import org.apache.spark.rdd.RDD

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

import org.cgnal.graphe.EnrichedRDD

private[graphe] case class GraphContainer[A, B](inputData: RDD[TinkerVertex], graph: SparkGraph[A, B]) {

  def unpersistInput(blocking: Boolean = true) = {
    inputData.unpersist(blocking)
    this
  }

}

object GraphContainer {

  def fromRawVertices[A, B](inputData: RDD[TinkerVertex])(implicit A: ClassTag[A], B: ClassTag[B], arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) = new GraphContainer[A, B](
    inputData.persisted(true),
    SparkBridge.asGraphX[A, B](inputData)
  )

}
