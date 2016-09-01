package org.cgnal

import java.util.UUID

import scala.collection.convert.decorateAsScala._
import scala.reflect.ClassTag

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.hadoop.fs.{ Path, FileSystem }
import org.apache.hadoop.conf.{ Configuration => HadoopConfig }

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx.{ EdgeTriplet => SparkEdgeTriplet, TripletFields, Graph }

/**
 * Contains API elements that help easily `load`, `save`, collect and aggregate edges, along with providing suffix
 * methods for copying and merging Hadoop and Tinkerpop configurations.
 */
package object graphe {

  private type TripletMap[A, B] = Map[(Long, Long, B), SparkEdgeTriplet[A, B]]

  private[graphe] def withReplacement(sparkContext: SparkContext, location: String, createDirs: Boolean = true)(f: String => Unit): Unit = {
    val newLocation = s"$location-${UUID.randomUUID().toString}"
    val locationPath     = new Path(location)
    val tempLocationPath = new Path(s"$location.trash")
    val newLocationPath  = new Path(newLocation)
    val fileSystem = FileSystem.get { sparkContext.hadoopConfiguration }

    if (createDirs) { fileSystem.mkdirs(newLocationPath) }
    f(newLocation)
    if (fileSystem.exists(locationPath)) { fileSystem.rename(locationPath, tempLocationPath) }
    fileSystem.rename(newLocationPath, locationPath)
    if (fileSystem.exists(tempLocationPath)) { fileSystem.delete(tempLocationPath, true) }
  }

  implicit class EnrichedRDD[A](rdd: RDD[A]) {

    def filterNot(f: A => Boolean) = rdd.filter { !f(_) }

  }

  /**
   * Implicit class to provide suffix method for loading graphs from a location on a filesystem.
   * @param sparkContext the enriched `SparkContext` instance
   */
  implicit class GraphESparkContext(sparkContext: SparkContext) {

    /**
     * Instantiates the facade that allows format selection.
     * @param location directory on the file-system where to find the graph data.
     * @tparam A vertex type.
     * @tparam B edge type.
     * @return `GraphLoader` facade that provides format selection functions.
     */
    def load[A, B](location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = GraphLoader[A, B](sparkContext, location)

  }

  /**
   * Implicit class adding methods to the standard spark `Graph`. Also adds suffix method to instantiate the saving
   * facade, which allows for saving the graph in a format on the file-system in the chosen location.
   * @param graph the enriched `Graph` instance.
   * @tparam A vertex type.
   * @tparam B edge type.
   */
  implicit class GraphEGraph[A, B](graph: Graph[A, B]) {

    /**
     * Returns the `SparkContext` of this graph.
     * @return `SparkContext` to which this graph belongs.
     */
    def sparkContext = graph.vertices.sparkContext

    /**
     * Instantiates the facade that allows format selection.
     * @param location directory on the file-system where to save the graph data.
     * @return `GraphSaver` facade that provides format selection functions.
     */
    def save(location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = GraphSaver(graph, location)

    /**
     * Aggregates `EdgeTriplet`s by sourceId, destinationId and the edge attribute.
     */
    def collectEdgeTriplets = graph.aggregateMessages[TripletMap[A, B]](
      ctx => {
        ctx.sendToSrc { Map { (ctx.srcId, ctx.dstId, ctx.attr) -> ctx.toEdgeTriplet } }
        ctx.sendToDst { Map { (ctx.srcId, ctx.dstId, ctx.attr) -> ctx.toEdgeTriplet } }
      },
      (a, b) => a ++ b,
      TripletFields.All
    )

  }

  /**
   * Enriches the standard hadoop configuration with some suffix methods.
   * @param hadoopConfig the enriched hadoop configuration.
   */
  implicit class EnrichedHadoopConfig(hadoopConfig: HadoopConfig) {

    /**
     * Copies the hadoop configuration onto a new one.
     * @return a new `Configuration` instance containing all the exact same configurations as `hadoopConfig`.
     */
    def copy = hadoopConfig.asScala.foldLeft(new HadoopConfig) { (accConf, entry) =>
      accConf.set(entry.getKey, entry.getValue)
      accConf
    }

    /**
     * Merges the given tinkerpop configuration into a '''copy''' of this hadoop configuration, overwriting existing
     * configurations.
     * @param tinkerConfig the tinkerpop configuration.
     * @return a copy of `hadoopConfiguration` containing all the configurations in `tinkerpopConfig`.
     */
    def mergeWith(tinkerConfig: TinkerConfig) = tinkerConfig.getKeys.asScala.foldLeft(copy) { (hadoop, next) =>
      hadoop.set(next, tinkerConfig.getProperty(next).toString)
      hadoop
    }

  }

}
