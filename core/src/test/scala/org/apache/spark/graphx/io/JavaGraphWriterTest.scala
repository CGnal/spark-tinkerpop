package org.apache.spark.graphx.io

import org.cgnal.common.domain.{Connection, Identifiable}

import scala.reflect.ClassTag

import org.scalatest.{ WordSpec, MustMatchers }

import org.apache.spark.test.{ TestSparkContext, SparkContextProvider }
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.helper.GraphWriterTestCases

class JavaGraphWriterTest
  extends WordSpec
  with MustMatchers
  with GraphWriterTestCases[JavaGraphWriter]
  with SparkContextProvider {

  protected def createWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = new JavaGraphWriter[A, B](graph, location)

  protected def readVertices[A<: Identifiable](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A]) =
    sparkContext.readObjectVertices[A](location)

  protected def readTriplets[A <: Identifiable, B <: Connection](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A], B: ClassTag[B]) =
    sparkContext.readObjectTriplets[A, B](location)

  "Java GraphWriter" must {

    "write java binary data" in test()

  }

}
