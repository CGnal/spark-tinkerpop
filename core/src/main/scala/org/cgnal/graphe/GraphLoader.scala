package org.cgnal.graphe

import scala.reflect.ClassTag

import org.apache.spark.SparkContext
import org.apache.spark.graphx.io.{ kryoSuffix, KryoGraphReader, javaSuffix, JavaGraphReader, jsonSuffix }
import org.apache.spark.graphx.serialization.kryo.KryoRegistry

import org.cgnal.graphe.tinkerpop.{ SparkBridge, Arrows, EnrichedTinkerSparkContext }

/**
 * Format selection facade for loading a spark graph from the file-system.
 * @param sparkContext the `SparkContext` instance used to create the graph.
 * @param location directory on the file-system where the graph is to be saved.
 * @param hasSrcId defines whether the `RDD` will include the IDs or not (`false` by default).
 * @param hasDstId defines whether the `RDD` will include the IDs or not (`false` by default).
 * @tparam A the vertex type.
 * @tparam B the edge type.
 */
final case class GraphLoader[A, B](sparkContext: SparkContext,
                                   location: String,
                                   hasSrcId: Boolean = false,
                                   hasDstId: Boolean = false)(implicit A: ClassTag[A], B: ClassTag[B]) {

  /**
   * Loads the graph from plain Java serialized data. This method, besides questionable performance problems, also has
   * obvious backward-compatibility issues.
   */
  def asJava = JavaGraphReader[A, B](sparkContext, s"$location/$javaSuffix").loadGraph(hasSrcId, hasDstId)

  /**
   * Loads the graph from Kyro serialized data, using custom Kryo serializers. (see KryoRegistry for details on how to
   * register custom vertex and edge type serializers).
   * @param registry the registry object that contains all necessary assignments for the serializer classes.
   */
  def asKryo(implicit registry: KryoRegistry) = KryoGraphReader[A, B](sparkContext, s"$location/$kryoSuffix", registry).loadGraph(hasSrcId, hasDstId)

  /**
   * Loads the graph vertices and edges by reading JSON data as defined by Tinkerpop. Note that no custom
   * JSON serialization is allowed, but selective mapping can be done through the implicitly passed `arrowV` and
   * `arrowE`.
   * @param arrowV the transformation function for vertex types.
   * @param arrowE the transformation function for edge types.
   */
  def asGraphSON(implicit arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) = SparkBridge.asGraphX[A, B] {
   sparkContext.loadGraphSON { s"$location/tinkerpop/$jsonSuffix" }
  }

  /**
   * Loads the graph vertices and edges by reading Kryo data as defined by Tinkerpop. Note that no custom
   * Kryo serialization is allowed, but selective mapping can be done through the implicitly passed `arrowV` and
   * `arrowE`.
   * @param arrowV the transformation function for vertex types.
   * @param arrowE the transformation function for edge types.
   */
  def asGryo(implicit arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) = SparkBridge.asGraphX[A, B] {
   sparkContext.loadGryo { s"$location/tinkerpop/$kryoSuffix" }
  }

}
