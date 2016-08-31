package org.cgnal.graphe.application

import scala.reflect.runtime.universe.TypeTag
import scala.util.{ Success, Failure, Try }

import org.apache.log4j.Logger

import org.apache.spark.rdd.RDD

import org.cgnal.graphe.application.config.ApplicationConfig

/**
 * Partial interface for all subclasses that can be run as applications.
 */
trait Application { this: SparkContextInstance =>

  /**
   * Logger dedicated to this application (not this instance). The name of the logger will reflect the name of the
   * application, given by the `--name` attribute.
   */
  protected lazy val logger = Logger.getLogger { s"cgnal.${this.getClass.getSimpleName}" }

  protected def show[A <: Product](rdd: RDD[A], lines: Int = 10)(implicit A: TypeTag[A]) = Try { rdd show lines }

  protected def timed[A](f: => Try[A]): Try[A] = timed(System.currentTimeMillis().toString) { f }

  protected def timed[A](s: String)(f: => Try[A]): Try[A] = {
    val startedAt = System.currentTimeMillis()
    val tag = s"[$s] >"
    logger.info(s"$tag Started timer")
    f match {
      case success @ Success(_) => logger.info { s"$tag Execution succeeded after [${(System.currentTimeMillis() - startedAt) / 1000d} second(s)]" }; success
      case failure @ Failure(_) => logger.info { s"$tag Execution failed after [${(System.currentTimeMillis() - startedAt) / 1000d} second(s)]" }; failure
    }

  }

  def config: ApplicationConfig

  /**
   * Running hook to be implemented by the inheriting classes, publicly exposed to be run by other classes or objects.
   * Note that hook is expected to return a `Try[Unit]` which implies that the execution of the hook can be
   * '''expected''' to side-effects.
   */
  def run(): Try[Unit]

  /**
   * Close hook which can be overridden by the inheriting classes, publicly exposes to be run by other classes or
   * objects. By default this will simply stop the current `SparkContext`.
   */
  def close(): Try[Unit] = Try {
    sparkContext.stop()
  }

}
