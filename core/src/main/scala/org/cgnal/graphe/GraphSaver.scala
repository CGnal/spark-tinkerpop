package org.cgnal.graphe

import scala.reflect.ClassTag

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.{ kryoSuffix, KryoGraphWriter, javaSuffix, JavaGraphWriter, jsonSuffix }
import org.apache.spark.graphx.serialization.kryo.KryoRegistry

import org.cgnal.graphe.tinkerpop.{ EnrichedSparkTinkerGraph, EnrichedTinkerEdgeRDD, Arrows }

/**
 * Format selection facade for saving a spark graph on the file-system.
 * @param graph the graph to be saved.
 * @param location directory on the file-system where the graph is to be saved.
 * @tparam A the vertex type.
 * @tparam B the edge type.
 */
final case class GraphSaver[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) {

  /**
   * Stores the graph vertices and edges by serializing in plain Java format. This method, besides questionable
   * performance problems, also has obvious backward-compatibility issues.
   */
  def asJava = withReplacement(graph.vertices.sparkContext, s"$location/$javaSuffix") { JavaGraphWriter(graph, _).saveGraph() }

  /**
   * Stores the graph vertices and edges by serializing in Kryo format using custom serializers (see KryoRegistry for
   * details on how to register custom vertex and edge type serializers).
   * @param registry the registry object that contains all necessary assignments for the serializer classes.
   */
  def asKryo(implicit registry: KryoRegistry) = withReplacement(graph.vertices.sparkContext, s"$location/$kryoSuffix") { KryoGraphWriter(graph, _, registry).saveGraph() }

  /**
   * Stores the graph vertices and edges by serializing in JSON format as defined by Tinkerpop. Note that no custom
   * JSON serialization is allowed, but selective mapping can be done through the implicitly passed `arrowV` and
   * `arrowE`.
   * @param arrowV the transformation function for vertex types.
   * @param arrowE the transformation function for edge types.
   */
  def asGraphSON(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = withReplacement(graph.vertices.sparkContext, s"$location/tinkerpop/$jsonSuffix", false) { graph.asTinkerpop.saveAsGraphSON }

  /**
   * Stores the graph vertices and edges by serializing in Kryo format as defined by Tinkerpop. Note that no custom
   * Kryo serialization is allowed, but selective mapping can be done through the implicitly passed `arrowV` and
   * `arrowE`.
   * @param arrowV the transformation function for vertex types.
   * @param arrowE the transformation function for edge types.
   */
  def asGryo(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = withReplacement(graph.vertices.sparkContext, s"$location/tinkerpop/$kryoSuffix", false) { graph.asTinkerpop.saveAsGryo }

}
