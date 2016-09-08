package org.apache.spark.graphx.io.helper

import org.apache.spark.rdd.RDD

import scala.language.higherKinds
import scala.reflect.ClassTag
import scala.util.Try

import org.scalatest.{ BeforeAndAfterAll, Suite }

import org.apache.hadoop.fs.Path

import org.apache.spark.graphx.{ EdgeTriplet, Graph }
import org.apache.spark.graphx.io.GraphWriter
import org.apache.spark.test.{ TestSparkContext, SparkContextProvider, EnrichedTestRDD }

import org.cgnal.common.domain._

trait GraphWriterTestCases[W[_, _] <: GraphWriter[_, _]] { this: SparkContextProvider with BeforeAndAfterAll with Suite =>

  protected def createWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]): W[A, B]

  protected def readVertices[A <: Identifiable](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A]): RDD[(Long, A)]
  protected def readTriplets[A <: Identifiable, B <: Connection](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A], B: ClassTag[B]): RDD[EdgeTriplet[A, B]]

  private def withLocation[U](f: String => U) = withFileSystem { fileSystem =>
    val location = s"test-data/${this.getClass.getSimpleName.toLowerCase}-${System.currentTimeMillis()}"
    val attempt  = Try { f(location) }
    fileSystem.delete(new Path(location), true)
    attempt.get
  }

  private def writeGraph(graph: Graph[User, Connection], location: String) = createWriter(graph, location).saveGraph()

  private def createUserGraph = withSparkContext { _.graph(UserGenerator.generator) }

  private def validateGraph(graph: Graph[User, Connection], location: String) = withSparkContext { sparkContext =>
    lazy val vertices = readVertices[User](sparkContext, location)
    lazy val edges    = readTriplets[User, Knows](sparkContext, location)

    vertices mustContainAll graph.vertices
    edges    mustContainAll graph.edges
  }

  protected def test() = withLocation { location =>
    val graph = createUserGraph
    writeGraph(graph, location)
    validateGraph(graph, location)
  }


}
