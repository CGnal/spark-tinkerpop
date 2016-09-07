package org.apache.spark.graphx.io

import org.apache.spark.graphx.io.helper.JavaGraphWriterTestCases
import org.scalatest.{ WordSpec, MustMatchers }

class JavaGraphWriterTest extends WordSpec with MustMatchers {

  "Java GraphWriter" must {

    "write java binary data" in JavaGraphWriterTestCases.test()

  }

}
