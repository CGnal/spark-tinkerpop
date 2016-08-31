package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

trait Config

trait ConfigReader[A <: Config] {

  def read(args: Seq[String]): A

}

trait ScallopConfigReader[A <: Config] { this: ConfigReader[A] =>

  final protected def default[B](b: B) = () => Some { b }

  protected def scallopts(scallop: Scallop): Scallop

  protected def consumeScallop(scallop: Scallop): A

  final def readScallop(scallop: Scallop): A  = consumeScallop {
    scallopts { scallop }
  }

  final def read(args: Seq[String]) = consumeScallop {
    scallopts { Scallop(args) }
  }

  final def withConfig[B](args: Seq[String])(f: A => B) = f { read(args) }

}
