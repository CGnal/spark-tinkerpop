package org.cgnal.common.domain

import org.cgnal.common.DataGenerator
import org.scalacheck.Gen

sealed trait Relationship

case object Friend extends Relationship

case object Acquaintance extends Relationship

case object Spouse extends Relationship

object RelationshipGenerator extends DataGenerator[Relationship] {

  def generator = Gen.oneOf(Friend, Acquaintance, Spouse)

}
