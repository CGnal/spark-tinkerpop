package org.cgnal.common

import org.scalacheck.Gen

trait DataGenerator[A] {

  protected def generator: Gen[A]

  def withSeq[B](length: Int)(f: Seq[A] => B) = f { generator.take(length) }

}
