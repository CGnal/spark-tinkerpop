package org.cgnal.graphe

import scala.reflect.ClassTag

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.{ kryoSuffix, KryoGraphWriter, javaSuffix, JavaGraphWriter, jsonSuffix }
import org.apache.spark.graphx.serialization.kryo.KryoRegistry

import org.cgnal.graphe.tinkerpop.{ EnrichedSparkTinkerGraph, EnrichedTinkerEdgeRDD, Arrows }

final case class GraphSaver[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) {

  def asJava = withReplacement(graph.vertices.sparkContext, s"$location/$javaSuffix") { JavaGraphWriter(graph, _).saveGraph() }

  def asKryo(implicit registry: KryoRegistry) = withReplacement(graph.vertices.sparkContext, s"$location/$kryoSuffix") { KryoGraphWriter(graph, _, registry).saveGraph() }

  def asGraphSON(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = withReplacement(graph.vertices.sparkContext, s"$location/tinkerpop/$jsonSuffix", false) { graph.asTinkerpop.saveAsGraphSON }

  def asGryo(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = withReplacement(graph.vertices.sparkContext, s"$location/tinkerpop/$kryoSuffix", false) { graph.asTinkerpop.saveAsGryo }

}
