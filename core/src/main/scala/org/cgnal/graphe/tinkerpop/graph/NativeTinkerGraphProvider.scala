package org.cgnal.graphe.tinkerpop.graph

import org.apache.hadoop.io.NullWritable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

import org.cgnal.graphe.tinkerpop.{ Arrows, TinkerpopEdges, NativeGraphInputFormat, EnrichedTinkerVertex }
import org.cgnal.graphe.{ EnrichedHadoopConfig, EnrichedRDD }

/**
 * Extends the base `TinkerGraphProvider` APIs to add a more efficient way of loading and saving Spark graph data into
 * a specific graph implementation using native code.
 */
trait NativeTinkerGraphProvider extends TinkerGraphProvider { this: Serializable =>

  /**
   * Loads the graph directly from the source into the given spark context, generating and `RDD[Vertex]` containing all
   * the tinkerpop vertices stored in the graph, together with the associated edges. You can further call
   * `SparkBridge.asGraphX` to convert this `RDD` into a Spark `Graph`.
   * @param sparkContext the `SparkContext` instance into which to load the graph
   * @return an `RDD[Vertex]` containing the tinkerpop vertices and edges
   */
  def loadNative(sparkContext: SparkContext): RDD[TinkerVertex]

  /**
   * Saves  the graph directly to the graph implementation, bypassing any unnecessary APIs and implementation layers.
   * Note that this can be a very low-level implementation, but should be general enough to account for any type of
   * vertices and edges.
   * @param rdd the Spark `RDD` to store
   * @param useTinkerpopKeys indicates whether the vertex creation should leave id generation to the graph libraries, or
   *                     use tinkerpop APIs to set the `id` key and value (typically by setting the  `T.id` property)
   * @param arrowV the vertex transformation function from `A` to a `Map`
   * @param arrowE the edge transformation function from `B` to a `Map`
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpopKeys: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B])

}

/**
 * Implements the loading side of the `NativeTinkerGraphProvider` by using a hadoop. The implementing class needs only
 * to supply the class of the `InputFormat` to use, which should extends the `NativeGraphInputFormat` alias, which in
 * turn resolves to `InputFormat[NullWritable, Vertex].`
 */
trait HadoopGraphLoader { this: NativeTinkerGraphProvider with Serializable =>

  /**
   * The class of `NativeInputFormat` to use for loading data from hadoop.
   */
  protected def nativeInputFormat: Class[_ <: NativeGraphInputFormat]

  def loadNative(sparkContext: SparkContext): RDD[TinkerVertex] = sparkContext.newAPIHadoopRDD[NullWritable, VertexWritable, NativeGraphInputFormat](
    sparkContext.hadoopConfiguration.withCredentials.mergeWith { tinkerConfig },
    nativeInputFormat.asInstanceOf[Class[NativeGraphInputFormat]],
    classOf[NullWritable],
    classOf[VertexWritable]
  ).map { _._2.get().asSpark }.asInstanceOf[RDD[TinkerVertex]]

}