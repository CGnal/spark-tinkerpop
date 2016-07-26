package org.cgnal.graphe

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.io.{ kryoSuffix, KryoGraphReader, javaSuffix, JavaGraphReader, jsonSuffix }
import org.apache.spark.graphx.serialization.kryo.KryoRegistry

import org.cgnal.graphe.tinkerpop.{ SparkBridge, Arrows, EnrichedTinkerSparkContext, NativeGraphInputFormat }
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

final case class GraphLoader[A, B](sparkContext: SparkContext,
                                   location: String,
                                   hasSrcId: Boolean = false,
                                   hasDstId: Boolean = false)(implicit A: ClassTag[A], B: ClassTag[B]) {

  def asJava = JavaGraphReader[A, B](sparkContext, s"$location/$javaSuffix").loadGraph(hasSrcId, hasDstId)

  def asKryo(implicit registry: KryoRegistry) = KryoGraphReader[A, B](sparkContext, s"$location/$kryoSuffix", registry).loadGraph(hasSrcId, hasDstId)

  def asGraphSON(implicit arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) = SparkBridge.asGraphX[A, B] {
   sparkContext.loadGraphSON { s"$location/tinkerpop/$jsonSuffix" }
  }

  def asGryo(implicit arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) = SparkBridge.asGraphX[A, B] {
   sparkContext.loadGryo { s"$location/tinkerpop/$kryoSuffix" }
  }

}

final case class NativeGraphLoader[NIO <: NativeGraphInputFormat](sparkContext: SparkContext)(implicit NIO: ClassTag[NIO]) {

  def as[A, B](implicit provider: TinkerGraphProvider, arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B], A: ClassTag[A], B: ClassTag[B]) = SparkBridge.asGraphX[A, B] {
    sparkContext.loadNative[NIO]
  }

}
