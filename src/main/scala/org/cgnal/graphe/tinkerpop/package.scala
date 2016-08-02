package org.cgnal.graphe

import scala.reflect.ClassTag
import scala.collection.convert.decorateAsScala._

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.hadoop.conf.{ Configuration => HadoopConfig }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputFormat

import org.apache.spark.graphx.{ EdgeTriplet => SparkEdgeTriplet, Graph => SparkGraph }
import org.apache.spark.graphx.io.{ Vertex => SparkVertex }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.{ GraphSONInputFormat, GraphSONOutputFormat }
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.{ GryoInputFormat, GryoOutputFormat }
import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Edge => TinkerEdge, Vertex => TinkerVertex }

import org.cgnal.graphe.tinkerpop.graph.{ EmptyTinkerGraphProvider, TinkerGraphProvider }

package object tinkerpop {

  type NativeGraphInputFormat = InputFormat[NullWritable, VertexWritable]

  def vertexAdditionNotSupported   = throw new UnsupportedOperationException("Vertex addition not supported")

  def edgeAdditionNotSupported     = throw new UnsupportedOperationException("Edge addition not supported")

  def vertexRemovalNotSupported    = throw new UnsupportedOperationException("Vertex removal not supported")

  def edgeRemovalNotSupported      = throw new UnsupportedOperationException("Edge removal not supported")

  def propertyAdditionNotSupported = throw new UnsupportedOperationException("Property addition not supported")

  def propertyRemovalNotSupported  = throw new UnsupportedOperationException("Property removal not supported")

  def propertyMissing(key: String) = throw new NoSuchElementException(s"No value for property [$key]")

  def computationNotSupported      = throw new UnsupportedOperationException("Graph computation not supported")

  implicit class EnrichedHadoopConfig(hadoopConfig: HadoopConfig) {

    def copy = hadoopConfig.asScala.foldLeft(new HadoopConfig) { (accConf, entry) =>
      accConf.set(entry.getKey, entry.getValue)
      accConf
    }

    def mergeWith(tinkerConfig: TinkerConfig) = tinkerConfig.getKeys.asScala.foldLeft(copy) { (hadoop, next) =>
      hadoop.set(next, tinkerConfig.getProperty(next).toString)
      hadoop
    }

  }

  implicit class EnrichedSparkTinkerGraph[A, B](graph: SparkGraph[A, B]) {

    def asTinkerpop(implicit A: ClassTag[A], B: ClassTag[B]) = SparkBridge.asTinkerpop(graph)

    def saveNativeGraph(useTinkerpop: Boolean = true)(implicit A: ClassTag[A], B: ClassTag[B], graphProvider: TinkerGraphProvider, arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = new EnrichedTinkerEdgeRDD(asTinkerpop).saveNative(useTinkerpop)

  }

  implicit class EnrichedSparkEdgeTriplet[A, B](edge: SparkEdgeTriplet[A, B]) {

    def asTinkerEdge(parentGraph: TinkerGraph)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = TinkerSparkEdge(
      edgeId        = TinkerSparkEdge.edgeId { edge }.toString,
      edgeLabel     = edge.attr.getClass.getSimpleName,
      parentGraph   = parentGraph,
      inVertexOpt   = Some { (edge.dstId, edge.dstAttr).asTinkerNeighborVertex(parentGraph) },
      outVertexOpt  = Some { (edge.srcId, edge.srcAttr).asTinkerNeighborVertex(parentGraph) },
      rawProperties = arrowE.apF { edge.attr }
    )

  }

  implicit class EnrichedSparkVertex[A](vertex: SparkVertex[A]) {

    def asTinkerPropertySet(implicit arrow: Arrows.TinkerVertexPropSetArrowF[A, AnyRef]) = arrow.apF(vertex._2)

    def asTinkerNeighborVertex(parentGraph: TinkerGraph, inEdge: Option[TinkerEdge] = None, outEdge: Option[TinkerEdge] = None)(implicit arrow: Arrows.TinkerVertexPropSetArrowF[A, AnyRef]) = TinkerSparkVertex(
      vertexId      = vertex._1,
      vertexLabel   = vertex._2.getClass.getSimpleName,
      parentGraph   = parentGraph,
      inEdges       = inEdge.toList,
      outEdges      = outEdge.toList,
      propertiesMap = asTinkerPropertySet
    )

  }

  implicit class EnrichedTinkerEdgeRDD[A, B](rdd: RDD[TinkerpopEdges[A, B]]) {

    def asVertexWritable(implicit graphProvider: TinkerGraphProvider, arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = graphProvider.withGraph { graph =>
      rdd.map { edges => NullWritable.get -> new VertexWritable(edges.asTinkerVertex(graph)) }
    }

    def saveAsGryo(path: String)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = asVertexWritable(EmptyTinkerGraphProvider, arrowV, arrowE).saveAsNewAPIHadoopFile[GryoOutputFormat](path)

    def saveAsGraphSON(path: String)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = asVertexWritable(EmptyTinkerGraphProvider, arrowV, arrowE).saveAsNewAPIHadoopFile[GraphSONOutputFormat](path)

    def saveNative(useTinkerpop: Boolean = true)(implicit graphProvider: TinkerGraphProvider, arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = rdd.foreachPartition {
      graphProvider.withGraphTransaction(_) { (graph, vertex) =>
        vertex.outEdges.foldLeft(graph.addVertex(vertex.asVertexKeyValue(useTinkerpop): _*)) { (v, e) =>
          val edge = e.asTinkerEdge(graph)(Arrows.tinkerVertexPropSetArrowF(arrowV), arrowE)
          v.addEdge(edge.edgeLabel, edge.inVertex(), Arrows.tinkerKeyValuePropSetArrowF(arrowE).apF(e.attr): _*)
          v
        }
      }
    }

  }

  implicit class EnrichedTinkerSparkContext(sparkContext: SparkContext) {

    def loadGraphSON(path: String): RDD[TinkerVertex] = sparkContext.newAPIHadoopFile[NullWritable, VertexWritable, GraphSONInputFormat](path).map { _._2.get() }

    def loadGryo(path: String): RDD[TinkerVertex] = sparkContext.newAPIHadoopFile[NullWritable, VertexWritable, GryoInputFormat](path).map { _._2.get() }

    def loadNative[A <: NativeGraphInputFormat](implicit A: ClassTag[A], provider: TinkerGraphProvider): RDD[TinkerVertex] = sparkContext.newAPIHadoopRDD[NullWritable, VertexWritable, A](
      sparkContext.hadoopConfiguration.mergeWith { provider.tinkerConfig },
      A.runtimeClass.asInstanceOf[Class[A]],
      classOf[NullWritable],
      classOf[VertexWritable]
    ).map { _._2.get() }

  }

}