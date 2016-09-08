package org.apache.spark

import org.apache.spark.rdd.RDD

package object test {

  implicit class EnrichedTestRDD[A](rdd: RDD[A]) {

    def containsAll[B](other: Seq[B]): Boolean = rdd.toLocalIterator.forall { other.contains }

    def mustContainAll[B](other: Seq[B]): Unit = if (!containsAll(other)) throw new RuntimeException("The RDD contains missing elements")

    def mustContainAll[B](other: RDD[B]): Unit = mustContainAll { other.toLocalIterator.toList }

    def toList   = rdd.toLocalIterator.toList

    def toStream = rdd.toLocalIterator.toSeq

  }

}
