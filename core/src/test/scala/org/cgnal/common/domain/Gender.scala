package org.cgnal.common.domain

import org.cgnal.common.DataGenerator
import org.scalacheck.Gen

sealed trait Gender

case object Male extends Gender

case object Female extends Gender

object GenderGenerator extends DataGenerator[Gender] {

  def generator = Gen.oneOf[Gender](Male, Female)

}
