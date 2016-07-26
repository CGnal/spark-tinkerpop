package org.cgnal.graphe.tinkerpop

import org.apache.spark.SparkContext
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

import scala.reflect.ClassTag
import scala.collection.convert.decorateAsScala._

import org.apache.hadoop.conf.{ Configuration => HadoopConfig }

import org.apache.spark.graphx.{ Graph => SparkGraph }
import org.apache.spark.rdd.RDD

import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex, Direction }

import org.cgnal.graphe.GraphEGraph

object SparkBridge {

  def asTinkerpop[A, B](graph: SparkGraph[A, B])(implicit A: ClassTag[A], B: ClassTag[B]) =
    graph.collectEdgeTriplets.join { graph.vertices }.map { case (vertexId, (edges, vertex)) => TinkerpopEdges(vertexId, vertex, edges.values.toList) }

  def asSparkVertices[A](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], arrow: Arrows.TinkerVertexArrowR[A]) = rdd.map { Arrows.tinkerSparkVertexArrowR.apR }

  def asSparkEdges[A](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], arrow: Arrows.TinkerEdgeArrowR[A]) = rdd.flatMap {
    _.edges(Direction.BOTH).asScala.map { Arrows.tinkerSparkEdgeArrowR.apR }
  }

  def asGraphX[A, B](rdd: RDD[TinkerVertex])(implicit A: ClassTag[A], B: ClassTag[B], arrowV: Arrows.TinkerVertexArrowR[A], arrowE: Arrows.TinkerEdgeArrowR[B]) =
    SparkGraph[A, B](asSparkVertices(rdd), asSparkEdges(rdd))

}
