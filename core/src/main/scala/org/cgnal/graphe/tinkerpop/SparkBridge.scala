package org.cgnal.graphe.tinkerpop

import scala.reflect.ClassTag
import scala.collection.convert.decorateAsScala._

import org.apache.spark.graphx.{ Graph => SparkGraph }
import org.apache.spark.rdd.RDD

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex, Direction }

import org.cgnal.graphe.GraphEGraph

/**
 * Provides functions that help converting from tinkerpop datastructures to Spark and vice-versa.
 */
object SparkBridge {

  /**
   * Collects the separate vertex and edge `RDD` information stored in the `graph` into one `RDD[TinkerpopEdges[A, B]]`.
   * @param graph the `Graph[A, B]` instance to be converted
   * @tparam A the vertex type
   * @tparam B the edge type
   * @return an `RDD[TinkerpopEdges[A, B]]`
   */
  def asTinkerpop[A, B](graph: SparkGraph[A, B])(implicit A: ClassTag[A], B: ClassTag[B]) =
    graph.collectEdgeTriplets.join { graph.vertices }.map { case (vertexId, (edges, vertex)) => TinkerpopEdges(vertexId, vertex, edges.values.toList) }

  /**
   * Uses implicit `Arrow`s to convert an `RDD[Vertex]` containing tinkerpop vertices into an `RDD[(Long, A)]`, which is
   * equivalent to a `VertexRDD[A]`
   * @tparam A the vertex type
   */
  def asSparkVertices[A](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], arrow: Arrows.TinkerVertexArrowR[A]) = rdd.map { Arrows.tinkerSparkVertexArrowR.apR }

  /**
   * Uses implicit `Arrow`s to convert an `RDD[Vertex]` containing tinkerpop vertices into an `RDD[Edge[A]]`
   * @tparam A the edge type
   */
  def asSparkEdges[A](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], arrow: Arrows.TinkerEdgeArrowR[A]) = rdd.flatMap {
    _.edges(Direction.OUT).asScala.map { Arrows.tinkerSparkEdgeArrowR.apR }
  }

  /**
   * Transforms an `RDD[Vertex` into a Spark `Graph[A, B]` using implemented `Arrow` transformations.
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  def asGraphX[A, B](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], B: ClassTag[B], arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) =
    SparkGraph[A, B](asSparkVertices(rdd), asSparkEdges(rdd))

}
