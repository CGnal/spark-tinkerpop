package org.cgnal.graphe.tinkerpop.graph

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{ Failure, Success, Try }

import org.slf4j.LoggerFactory

trait TransactionWrapper { this: Serializable =>

  protected lazy val log = LoggerFactory.getLogger(s"transaction.${this.getClass.getSimpleName.filter { _.isLetterOrDigit }}")

  def commit(): Unit

  def rollback(): Unit

  def open(): Unit

  def close(): Unit

  def isOpen: Boolean

  def isClosed: Boolean = !isOpen

  final protected def withDebug[U](m: String)(f: => U) = {
    log.debug(m)
    f
  }

  final protected def sleepWarning(retryDelay: FiniteDuration)(m: String) = {
    log.warn(m)
    Thread.sleep(retryDelay.toMillis)
  }

  final protected def attemptOpen() = Try {
    if (isClosed) open()
  }

  final protected def attemptRollback(error: Throwable) = Try {
    log.error("Execution failed within transaction: rolling back", error)
    rollback()
    throw error
  }

  final def attemptTransaction[U](retryThreshold: Int, retryDelay: FiniteDuration)(f: TransactionWrapper => Try[U]): Try[U] = attempt(retryThreshold, retryDelay) { f(this) }

  final def attempt[U](retryThreshold: Int, retryDelay: FiniteDuration)(f: => Try[U]): Try[U] = for {
    _      <- attemptOpen()
    result <- f.recoverWith { case error => attemptRollback(error) }
    _      <- Success { attemptCommit(retryThreshold, retryDelay) }
  } yield result

  final def attempt[U](f: => Try[U]): Try[U] = attempt(5, 1.second)(f)

  final def attemptCommit(retryThreshold: Int = 5, retryDelay: FiniteDuration = 1.second) = _attemptCommit(retryThreshold, retryDelay)

  @tailrec
  private def _attemptCommit(retryThreshold: Int, retryDelay: FiniteDuration, attempt: Int = 1): Unit = Try { commit() } match {
    case Success(_)                             =>
      log.info(s"Committed transaction at attempt [$attempt] - closing")
      close()
    case Failure(e) if attempt > retryThreshold =>
      rollback()
      close()
      throw new RuntimeException(s"Unable to commit transaction after [$attempt] attemp(s) -- rolling back", e)
    case Failure(_)                             =>
      sleepWarning(retryDelay) { s"Failed to commit transaction after attempt [$attempt] -- backing off for [${retryDelay.toSeconds}] second(s)" }
      _attemptCommit(retryThreshold, retryDelay, attempt + 1)
  }

}
