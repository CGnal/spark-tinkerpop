package org.apache.spark.test

import org.apache.spark.graphx.{EdgeTriplet, Graph, Edge, VertexRDD}
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
      case (_, (a1, a2)) if a1 == a2 => Seq.empty[Edge[Connection]]
      case (_, (a1, a2))             => Seq(Edge(a1.id, a2.id, a1 relatesTo a2), Edge(a1.id, a2.id, a2 relatesTo a1))
    }
  }

  def graph[A <: Identifiable](gen: Gen[A], numVertices: Int = 100, numGroups: Int = 30)(implicit A: ClassTag[A]) = {
    val vertexRDD = vertices(gen, numVertices)
    val edgeRDD   = edges(vertexRDD, numGroups)
    Graph[A, Connection](vertexRDD, edgeRDD)
  }

  def readObjectVertices[A <: Identifiable](location: String)(implicit A: ClassTag[A]) = sparkContext.objectFile[(Long, A)](s"$location/$vertexLocation")

  def readObjectTriplets[A <: Identifiable, B <: Connection](location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = sparkContext.objectFile[EdgeTriplet[A, B]](s"$location/$tripletLocation")

}
