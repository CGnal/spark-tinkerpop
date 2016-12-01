package org.cgnal.graphe

import scala.collection.convert.decorateAsScala._
import scala.reflect.ClassTag

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.InputFormat

import org.apache.spark.graphx.{ EdgeTriplet => SparkEdgeTriplet, Graph => SparkGraph }
import org.apache.spark.graphx.io.{ Vertex => SparkVertex }
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.hadoop.structure.io.graphson.{ GraphSONInputFormat, GraphSONOutputFormat }
import org.apache.tinkerpop.gremlin.hadoop.structure.io.gryo.{ GryoInputFormat, GryoOutputFormat }
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Edge => TinkerEdge, Vertex => TinkerVertex, VertexProperty => TinkerProperty, T }
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph

import org.cgnal.graphe.tinkerpop.graph.{ NativeTinkerGraphProvider, EmptyTinkerGraphProvider, TinkerGraphProvider }

package object tinkerpop {

  /**
   * Alias for `InputFormat[NullWritable, VertexWritable]`.
   */
  type NativeGraphInputFormat = InputFormat[NullWritable, VertexWritable]

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def vertexAdditionNotSupported   = throw new UnsupportedOperationException("Vertex addition not supported")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def edgeAdditionNotSupported     = throw new UnsupportedOperationException("Edge addition not supported")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def vertexRemovalNotSupported    = throw new UnsupportedOperationException("Vertex removal not supported")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def edgeRemovalNotSupported      = throw new UnsupportedOperationException("Edge removal not supported")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def propertyAdditionNotSupported = throw new UnsupportedOperationException("Property addition not supported")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def propertyRemovalNotSupported  = throw new UnsupportedOperationException("Property removal not supported")

  /**
   * Throws a `NoSuchElementException` with a relevant error message.
   * @param key the missing key
   */
  def propertyMissing(key: String) = throw new NoSuchElementException(s"No value for property [$key]")

  /**
   * Throws an `UnsupportedOperationException` with a relevant error message.
   */
  def computationNotSupported      = throw new UnsupportedOperationException("Graph computation not supported")

  /**
   * Enriches a spark `Graph[A, B]` instance to add suffix methods to easily transform and use graph data.
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  implicit class EnrichedSparkTinkerGraph[A, B](graph: SparkGraph[A, B]) {

    /**
     * Collects all vertex and edge information into one `RDD`, thereby facilitating the link between spark and
     * tinkerpop.
     * @param A the vertex type
     * @param B the edge type
     * @return an `RDD[TinkerpopEdges[A, B]]`
     */
    def asTinkerpop(implicit A: ClassTag[A], B: ClassTag[B]) = SparkBridge.asTinkerpop(graph)

    /**
     * Saves the graph using the implicit `NativeTinkerGraphProvider` instance.
     * @param useTinkerpop indicates whether id generation is done explicitly using tinkerpop APIs (typically using
     *                     `T.id`) or is delegated to the graph system.
     * @param graphProvider the `NativeGraphProvider` instance that is implements the saving operation.
     * @param arrowV the transformation arrow `A >-> Map[String, AnyRef]`
     * @param arrowE the transformation arrow `B >-> Map[String, AnyRef]`
     */
    def saveNativeGraph(useTinkerpop: Boolean = true)(implicit A: ClassTag[A], B: ClassTag[B], graphProvider: NativeTinkerGraphProvider, arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = new EnrichedTinkerEdgeRDD(asTinkerpop).saveNative(useTinkerpop)

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
      inEdges       = inEdge.toArray,
      outEdges      = outEdge.toArray,
      propertiesMap = asTinkerPropertySet
    )
  }

  implicit class EnrichedTinkerEdgeRDD[A, B](rdd: RDD[TinkerpopEdges[A, B]]) {

    def asVertexWritable(implicit graphProvider: TinkerGraphProvider, arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = graphProvider.withGraph { graph =>
      rdd.map { edges => NullWritable.get -> new VertexWritable(edges.asTinkerVertex(graph)) }
    }

    def saveAsGryo(path: String)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = asVertexWritable(EmptyTinkerGraphProvider, arrowV, arrowE).saveAsNewAPIHadoopFile[GryoOutputFormat](path)

    def saveAsGraphSON(path: String)(implicit arrowV: Arrows.TinkerVertexPropSetArrowF[A, AnyRef], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = asVertexWritable(EmptyTinkerGraphProvider, arrowV, arrowE).saveAsNewAPIHadoopFile[GraphSONOutputFormat](path)

    def saveNative(useTinkerpop: Boolean = false)(implicit graphProvider: NativeTinkerGraphProvider, arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B]) = graphProvider.saveNative(rdd)
  }

  implicit class EnrichedTinkerSparkContext(sparkContext: SparkContext) {

    def loadGraphSON(path: String): RDD[TinkerVertex] = sparkContext.newAPIHadoopFile[NullWritable, VertexWritable, GraphSONInputFormat](path).map { _._2.get() }

    def loadGryo(path: String): RDD[TinkerVertex] = sparkContext.newAPIHadoopFile[NullWritable, VertexWritable, GryoInputFormat](path).map { _._2.get() }

    def loadNative[A, B](persist: Boolean = true)(implicit provider: NativeTinkerGraphProvider, arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B], A: ClassTag[A], B: ClassTag[B]) =
      SparkBridge.asGraphX[A, B] {
        if (persist) provider.loadNative(sparkContext).persisted()
        else         provider.loadNative(sparkContext)
      }
  }

  implicit class EnrichedGraphTraversal[A, B](traversal: GraphTraversal[A, B]) {

    /**
     * Creates an `Iterator` from a `GraphTraversal` instance, applying an arbitrary function `f` to all the elements.
     */
    def asIterator[C](f: B => C): Iterator[C] = new Iterator[C] {
      def hasNext = traversal.hasNext
      def next()  = f { traversal.next() }
    }

    /**
     * Creates an `Iterator` from a `GraphTraversal` instance.
     */
    def asIterator: Iterator[B] = asIterator[B] { identity }

    /**
     * Applies `f` on this instance as an `Iterator` assuming that the traversal is not empty, in which case `None` is
     * returned.
     */
    def unlessEmpty[C](f: Iterator[B] => C): Option[C] = if (traversal.hasNext) Option { f(asIterator) } else None

  }

  implicit class EnrichedStarGraph(graph: StarGraph) {

    def addVertex(vertex: TinkerVertex) = graph.addVertex(T.id, vertex.id, T.label, vertex.label) copy vertex

  }

  implicit class EnrichedTinkerVertex(vertex: TinkerVertex) {

    def asSpark = TinkerSparkVertex.fromTinkerpop(vertex)

    /**
     * Copies the properties of `other` onto `this`.
     */
    def copy(other: TinkerVertex) = other.properties[AnyRef]().asScala.foldLeft(vertex) { (vv, prop) =>
      vv.property(TinkerProperty.Cardinality.list, prop.key(), prop.value(), T.id, prop.id())
      prop.properties[AnyRef]().asScala.foreach { metaProp => prop.property(metaProp.key(), metaProp.value) }
      vv
    }

    /**
     * Adds an out-edge to this `Vertex` instance from another `Edge`, returning `this` as the `outVertex` of the
     * resulting `Edge`, thereby effectively copying the given `Edge` as an out-edge to this `Vertex`.
     */
    def addOutEdge(edge: TinkerEdge) = vertex.addEdge(
      edge.label(),
      edge.inVertex(),
      { T.id +: edge.id() +: edge.properties[AnyRef]().asScala.toSeq.flatMap { prop => Seq(prop.key(), prop.value()) } }: _*
    ).outVertex

    /**
     * Adds an in-edge to this `Vertex` instance from another `Edge`, returning `this` as the `inVertex` of the
     * resulting `Edge`, thereby effectively copying the given `Edge` as an in-edge to this `Vertex`.
     */
    def addInEdge(edge: TinkerEdge) = vertex.graph.addVertex(edge.outVertex()).addEdge(
      edge.label,
      vertex,
      { T.id +: edge.id() +: edge.properties[AnyRef]().asScala.toSeq.flatMap { prop => Seq(prop.key(), prop.value()) } }: _*
    ).inVertex

    /**
     * Adds an arbitrary edge as either an in- or an out-edge, depending on which end of the edge this vertex sits at.
     */
    def addEdge(edge: TinkerEdge) = if (edge.outVertex.id == vertex.id) addOutEdge(edge) else addInEdge(edge)

  }

}