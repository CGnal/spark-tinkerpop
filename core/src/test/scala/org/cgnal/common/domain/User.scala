package org.cgnal.common.domain

import org.scalacheck.Gen

import scala.io.Source

import org.cgnal.common.DataGenerator

import scala.util.Random

case class User(id: Long, name: String, gender: Gender) extends Identifiable {

  def relatesTo[B <: Identifiable](other: B) = Knows(this.id, other.id)

}

object UserGenerator extends DataGenerator[User] {

  private lazy val maleNamesFile   = this.getClass.getResourceAsStream("/male-names.csv")
  private lazy val femaleNamesFile = this.getClass.getResourceAsStream("/female-names.csv")

  private lazy val maleNames   = Source.fromInputStream { maleNamesFile   }.getLines()
  private lazy val femaleNames = Source.fromInputStream { femaleNamesFile }.getLines()

  private lazy val maleNameGen   = Gen.oneOf[String] { maleNames.toSeq   }
  private lazy val femaleNameGen = Gen.oneOf[String] { femaleNames.toSeq }

  private def generateName(gender: Gender) = gender match {
    case Male   => maleNameGen
    case Female => femaleNameGen
  }

  private def generateId(name: String) = Gen.posNum[Long].map { long =>
    Random.nextLong() + long + name.hashCode
  }

  def generator = for {
    gender <- GenderGenerator.generator
    name   <- generateName(gender)
    id     <- generateId(name)
  } yield User(id, name, gender)

}
