package org.apache.spark.test

import org.apache.hadoop.fs.FileSystem
import org.scalatest.{ Suite, BeforeAndAfterAll }

trait SparkContextProvider extends BeforeAndAfterAll { this: Suite =>

  protected def numThreads: Int = 1

  protected def startup(): Unit = { }

  protected def shutdown(): Unit = { }

  private lazy val sparkContext = new TestSparkContext(this.getClass.getSimpleName, numThreads)

  final override protected def beforeAll() = {
    sparkContext.startup()
    startup()
  }

  final override protected def afterAll() = {
    sparkContext.shutdown()
    shutdown()
  }

  final protected def withSparkContext[A](f: TestSparkContext => A) = f { sparkContext }

  final protected def withFileSystem[A](f: FileSystem => A) = f { sparkContext.fileSystem }

}
