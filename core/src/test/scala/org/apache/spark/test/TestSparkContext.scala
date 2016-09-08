package org.apache.spark.test

import scala.reflect.ClassTag

import org.scalacheck.Gen

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.io.{ BytesWritable, NullWritable }

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.graphx.io._
import org.apache.spark.graphx.{ EdgeTriplet, Graph, Edge }
import org.apache.spark.graphx.serialization.kryo.{ KryoRegistry, KryoSerde }
import org.apache.spark.rdd.RDD

import org.cgnal.common.CGnalGen
import org.cgnal.common.domain.{ Connection, Knows, Identifiable }

final class TestSparkContext(conf: SparkConf, numThreads: Int) {

  private var isUp = false

  def this(applicationName: String, numThreads: Int = 1) = this(new SparkConf().setAppName(applicationName), numThreads)

  private lazy val _sparkContext = new SparkContext(
    conf.setMaster(s"local[$numThreads]")
  )

  private lazy val _fileSystem = FileSystem.get(_sparkContext.hadoopConfiguration)

  private def whenStarted[U](f: => U) = if (isUp) f else throw new IllegalStateException("SparkContext is not started!")

  def startup(): Unit = {
    _sparkContext
    isUp = true
  }

  def shutdown(): Unit = if (isUp) {
    _sparkContext.stop()
    isUp = false
  }

  def sparkContext = whenStarted { _sparkContext }
  def fileSystem   = whenStarted { _fileSystem   }

  def rdd[A](seq: Seq[A])(implicit A: ClassTag[A]): RDD[A] = sparkContext.parallelize(seq)

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

  def readKryoVertices[A <: Identifiable](location: String, registry: KryoRegistry)(implicit A: ClassTag[A]) = sparkContext.sequenceFile(
    s"$location/$vertexLocation",
    classOf[NullWritable],
    classOf[BytesWritable]
  ).flatMap { case (_, bytes) => KryoSerde.read[Array[(Long, A)]](registry)(bytes.getBytes) }

  def readObjectTriplets[A <: Identifiable, B <: Connection](location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = sparkContext.objectFile[EdgeTriplet[A, B]](s"$location/$tripletLocation")

  def readKryoTriplets[A <: Identifiable, B <: Connection](location: String, registry: KryoRegistry)(implicit A: ClassTag[A], B: ClassTag[B]) = sparkContext.sequenceFile(
    s"$location/$tripletLocation",
    classOf[NullWritable],
    classOf[BytesWritable]
  ).flatMap { case (_, bytes) => KryoSerde.read[Array[EdgeTriplet[A, B]]](registry)(bytes.getBytes) }

}
