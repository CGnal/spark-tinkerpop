package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import org.scalatest.{ WordSpec, MustMatchers }

import org.apache.spark.graphx.serialization.kryo.TestKryoRegistry
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.io.helper.GraphWriterTestCases
import org.apache.spark.test.{ TestSparkContext, SparkContextProvider }

import org.cgnal.common.domain.{ Connection, Identifiable }

class KryoGraphWriterTest
  extends WordSpec
  with MustMatchers
  with GraphWriterTestCases[KryoGraphWriter]
  with SparkContextProvider {

  protected def createWriter[A, B](graph: Graph[A, B], location: String)(implicit A: ClassTag[A], B: ClassTag[B]) = new KryoGraphWriter[A, B](graph, location, TestKryoRegistry)

  protected def readVertices[A <: Identifiable](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A]) =
    sparkContext.readKryoVertices[A](location, TestKryoRegistry)

  protected def readTriplets[A <: Identifiable, B <: Connection](sparkContext: TestSparkContext, location: String)(implicit A: ClassTag[A], B: ClassTag[B]) =
    sparkContext.readKryoTriplets[A, B](location, TestKryoRegistry)

  "Kryo GraphWriter" must {

    "write java binary data" in test()

  }

}
