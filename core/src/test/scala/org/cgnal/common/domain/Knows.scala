package org.cgnal.common.domain

import org.cgnal.common.CGnalGen

case class Knows(source: Long, destination: Long, relationship: Relationship) extends Connection

object Knows {

  def apply(source: Long, destination: Long): Knows = Knows(source, destination, RelationshipGenerator.generator.generate)

}
