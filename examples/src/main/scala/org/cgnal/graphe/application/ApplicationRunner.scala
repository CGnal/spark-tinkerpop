package org.cgnal.graphe.application

import scala.util.{ Success, Failure }

import org.slf4j.LoggerFactory

object ApplicationRunner {

  private lazy val log = LoggerFactory.getLogger("cgnal.application.Runner")

  private def chooseApplication(name: String, args: Seq[String]) = name.toLowerCase match {
    case "titan"    => TitanApplication.create(args)
    case "graphson" => GraphSONApplication.create(args)
    case other      => Failure { new IllegalArgumentException(s"Invalid application name [$other]") }
  }

  private def createApplication(args: Array[String]) = args.toList match {
    case head :: tail => chooseApplication(head, tail)
    case Nil          => Failure { new IllegalArgumentException("Invalid empty argument list -- you should supply the application name followed by the appropriate list of argument") }
  }

  private def safeMain(args: Array[String]) = for {
    application <- createApplication(args)
    _           <- application.start()
    _           <- application.close()
  } yield ()

  def main(args: Array[String]) = safeMain(args) match {
    case Success(application) => log.info("Application finished and closed")
    case Failure(error)       => log.error("Application failed due to exception:", error); sys.exit(1)
  }


}
