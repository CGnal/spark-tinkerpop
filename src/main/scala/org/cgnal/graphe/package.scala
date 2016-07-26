package org.cgnal

import java.util.UUID

import scala.reflect.ClassTag

import org.apache.hadoop.fs.{ Path, FileSystem }

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{ EdgeTriplet => SparkEdgeTriplet, TripletFields, Graph }

import org.cgnal.graphe.tinkerpop.NativeGraphInputFormat

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

  implicit class GraphESparkContext(sparkContext: SparkContext) {

    def loadGraph[A, B](location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = GraphLoader[A, B](sparkContext, location)

    def loadNativeGraph[A <: NativeGraphInputFormat](implicit A: ClassTag[A]) = NativeGraphLoader[A](sparkContext)

  }

  implicit class GraphEGraph[A, B](graph: Graph[A, B]) {

    def sparkContext = graph.vertices.sparkContext

    def save(location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = GraphSaver(graph, location)

    def collectEdgeTriplets = graph.aggregateMessages[TripletMap[A, B]](
      ctx => {
        ctx.sendToSrc { Map { (ctx.srcId, ctx.dstId, ctx.attr) -> ctx.toEdgeTriplet } }
        ctx.sendToDst { Map { (ctx.srcId, ctx.dstId, ctx.attr) -> ctx.toEdgeTriplet } }
      },
      (a, b) => a ++ b,
      TripletFields.All
    )

  }



}
