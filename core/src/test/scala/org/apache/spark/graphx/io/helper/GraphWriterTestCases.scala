package org.apache.spark.graphx.io.helper

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.GraphWriter
import org.apache.spark.test.TestSparkContext
import org.cgnal.common.domain.{Knows, User, Connection, UserGenerator}
import org.apache.spark.test.EnrichedTestRDD

import scala.language.higherKinds
import scala.reflect.ClassTag

trait GraphWriterTestCases[W[_, _] <: GraphWriter[_, _]] {

  private lazy val sparkContext = new TestSparkContext(this.getClass.getSimpleName, 1)

  private def withLocation[U](f: String => U) = f { s"test-data/${this.getClass.getSimpleName.toLowerCase}-${System.currentTimeMillis()}" }

  protected def createWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]): W[A, B]

  private def createUserGraph = sparkContext.graph(UserGenerator.generator)

  private def writeGraph(graph: Graph[User, Connection], location: String) = createWriter(graph, location).saveGraph()

  private def validateGraph(graph: Graph[User, Connection], location: String) = {
    lazy val vertices = sparkContext.readObjectVertices[User](location)
    lazy val edges    = sparkContext.readObjectTriplets[User, Knows](location)

    vertices mustContainAll graph.vertices
    edges    mustContainAll graph.edges
  }

  def test() = withLocation { location =>
    val graph = createUserGraph
    writeGraph(graph, location)
    validateGraph(graph, location)
  }


}
