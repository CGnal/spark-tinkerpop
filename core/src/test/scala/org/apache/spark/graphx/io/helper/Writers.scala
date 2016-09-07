package org.apache.spark.graphx.io.helper

import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.JavaGraphWriter

import scala.reflect.ClassTag

object JavaGraphWriterTestCases extends GraphWriterTestCases[JavaGraphWriter] {

  protected def createWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = new JavaGraphWriter[A, B](graph, location)

}
