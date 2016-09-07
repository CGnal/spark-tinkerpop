package org.cgnal

import org.scalacheck.Gen

package object common {

  implicit class CGnalGen[A](gen: Gen[A]) {

    def generate(maxAttempts: Int): A = Stream.continually { gen.sample }.zipWithIndex.dropWhile { case (res, attempts) =>
      res.isEmpty && { attempts < maxAttempts }
    }.headOption match {
      case Some((Some(a), _)) => a
      case _                  => throw new RuntimeException(s"Unable to get a valid sample within [$maxAttempts] attempt(s)")
    }

    def generate: A = generate(100)

    def take(num: Int): Seq[A] = new CGnalGen(Gen.listOfN(num, gen)).generate

  }

}
