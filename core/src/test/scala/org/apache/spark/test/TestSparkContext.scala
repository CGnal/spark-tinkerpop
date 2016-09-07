package org.apache.spark.test

import org.apache.spark.graphx.{Graph, Edge, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD
import org.cgnal.common.domain.{Connection, Knows, Identifiable}

import scala.reflect.ClassTag

import org.scalacheck.Gen

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx.io._

import org.cgnal.common.CGnalGen

final class TestSparkContext(conf: SparkConf, numThreads: Int) {

  def this(applicationName: String, numThreads: Int = 1) = this(new SparkConf().setAppName(applicationName), numThreads)

  private lazy val _sparkContext = new SparkContext(
    conf.setMaster(s"local[$numThreads]")
  )

  def sparkContext = _sparkContext

  def rdd[A](seq: Seq[A])(implicit A: ClassTag[A]): RDD[A] = _sparkContext.parallelize(seq)

  def rdd[A](gen: Gen[A], length: Int = 100)(implicit A: ClassTag[A]): RDD[A] = rdd { gen take length }

  def vertices[A <: Identifiable](gen: Gen[A], length: Int = 100)(implicit A: ClassTag[A]): RDD[(Long, A)] = rdd(gen, length).map { a => (a.id, a)  }

  def edges[A <: Identifiable](vertices: RDD[(Long, A)], groups: Int = 30)(implicit A: ClassTag[A]): RDD[Edge[Connection]] = {
    val binned = vertices.map { case (id, a) => (id % groups, a) }
    binned.join(binned).flatMap {
      case (_, (user1, user2)) if user1 == user2 => Seq.empty[Edge[Connection]]
      case (_, (user1, user2))                   => Seq(Edge(user1.id, user2.id, user1 relatesTo user2), Edge(user2.id, user1.id, user2 relatesTo user1))
    }
  }

  def graph[A <: Identifiable](gen: Gen[A], numVertices: Int = 100, numGroups: Int = 30)(implicit A: ClassTag[A]) = {
    val vertexRDD = vertices(gen, numVertices)
    val edgeRDD   = edges(vertexRDD, numGroups)
    Graph(vertexRDD, edgeRDD)
  }

  def readObjectVertices[A <: Identifiable](location: String)(implicit A: ClassTag[A]) = sparkContext.objectFile[A](s"$location/$vertexLocation")

  def readObjectEdges[A <: Connection](location: String)(implicit A: ClassTag[A]) = sparkContext.objectFile[A](s"$location/$edgeLocation")

}
