package org.cgnal.graphe.application


import java.io.{BufferedOutputStream, OutputStream, PrintStream}

import scala.reflect.runtime.universe.TypeTag
import scala.util.{ Success, Failure, Try }

import org.slf4j.LoggerFactory

import org.apache.hadoop.security.UserGroupInformation

import org.apache.spark.rdd.RDD

import org.cgnal.graphe.application.config.{NoSecurityConfig, KerberosConfig, ApplicationConfig}

/**
 * Partial interface for all subclasses that can be run as applications. For a class to be an `Application` means that
 * it has access to a `SparkContext` instance via the `SparkContextInstance` trait mix-in.
 */
trait Application { this: SparkContextInstance =>

  /**
   * Logger dedicated to this application (not this instance). The name of the logger will reflect the name of the
   * application, given by the `--name` attribute.
   */
  protected lazy val log = LoggerFactory.getLogger { s"cgnal.${this.getClass.getSimpleName}" }

  /**
   * Utility method for applciation instances to show the contexts of `RDD`s whose elements are `Product`s. This will
   * automatically print out the first `lines` (default 10) elements of the `RDD` to the logger's INFO print stream.
   * @param rdd
   * @param lines
   * @param A
   * @tparam A
   * @return
   */
  final protected def show[A <: Product](rdd: RDD[A], lines: Int = 10)(implicit A: TypeTag[A]) = Try { rdd.show(lines, Slf4jOutputStream.info(log).asStream) }

  final protected def timed[A](f: => Try[A]): Try[A] = timed(System.currentTimeMillis().toString) { f }

  final protected def timed[A](s: String)(f: => Try[A]): Try[A] = {
    val startedAt = System.currentTimeMillis()
    val tag = s"[$s] >"
    log.info(s"$tag Started timer")
    f match {
      case success @ Success(_) => log.info { s"$tag Execution succeeded after [${(System.currentTimeMillis() - startedAt) / 1000d} second(s)]" }; success
      case failure @ Failure(_) => log.info { s"$tag Execution failed after [${(System.currentTimeMillis() - startedAt) / 1000d} second(s)]" }; failure
    }

  }

  def config: ApplicationConfig

  /**
   * Running hook to be implemented by the inheriting classes, publicly exposed to be run by other classes or objects.
   * Note that hook is expected to return a `Try[Unit]` which implies that the execution of the hook can be
   * '''expected''' to side-effects.
   */
  protected def run(): Try[Unit]

  /**
   * Starts the application by first trying to secure the application and then by calling `run()`.
   */
  final def start() = for {
    _ <- Try { log.info(s"Running application [${if (config.isSecured) "in secured mode" else "in non-secured mode"}]") }
    _ <- run()
  } yield ()

  /**
   * Close hook which can be overridden by the inheriting classes, publicly exposes to be run by other classes or
   * objects. By default this will simply stop the current `SparkContext`.
   */
  def close(): Try[Unit] = Try {
    //sparkContext.stop()
  }

}
