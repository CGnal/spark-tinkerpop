package org.cgnal.graphe.tinkerpop.graph

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{ Random, Failure, Success, Try }

import org.slf4j.LoggerFactory

/**
 * A Serializable wrapper over transactional elements that is not necessarily bound to tinkerpop transactions, but
 * can also extend to native graph transaction types. This wrapper adds a simple reattempt strategy when committing.
 */
trait TransactionWrapper { this: Serializable =>

  protected lazy val log = LoggerFactory.getLogger(s"cgnal.transaction.${this.getClass.getSimpleName.filter { _.isLetterOrDigit }}")

  def commit(): Unit

  def rollback(): Unit

  def open(): Unit

  def close(): Unit

  def isOpen: Boolean

  def isClosed: Boolean = !isOpen

  private def randomDelay = { Random.nextInt(1000) + 1000 }.milliseconds

  /**
   * Logs a debug line before executing `f`.
   * @param m the log message
   * @param f the delayed function block
   */
  final protected def withDebug[U](m: String)(f: => U) = {
    log.debug(m)
    f
  }

  /**
   * Logs a warning message before sleeping the current thread for a specific amount of time
   * @param retryDelay the amount of time to sleep in `FiniteDuration`
   * @param m the log message
   */
  final protected def sleepWarning(error: Throwable, retryDelay: FiniteDuration)(m: String) = {
    log.warn(m, error)
    Thread.sleep(retryDelay.toMillis)
  }

  /**
   * Tries to open the transaction.
   */
  final protected def attemptOpen() = Try {
    if (isClosed) open()
  }

  /**
   * Tries to rollback the transaction before rethrowing `error`.
   * @param error the error which triggered the rollback
   */
  final protected def attemptRollback(error: Throwable) = Try {
    log.error("Execution failed within transaction: rolling back", error)
    rollback()
    throw error
  }

  /**
   * Attempts to apply `f` onto `this` before committing in case of `Success` or rolling back in case of `Failure`.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   * @param closeWhenDone indicates whether the transaction should be closed when the commit is successful or not; note
   *                      the transaction is always closed automatically when a rollback is performed after `f` fails
   * @param f the function to apply on `this` wrapper
   */
  final def attemptTransaction[U](retryThreshold: Int, retryDelay: FiniteDuration, closeWhenDone: Boolean = true)(f: TransactionWrapper => Try[U]): Try[U] =
    attempt(retryThreshold, retryDelay, closeWhenDone) { f(this) }

  /**
   * Applies `f` and then tries to commit the transaction if the result of `f` was a `Success`, or rollback in case of
   * `Failure`.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   * @param closeWhenDone indicates whether the transaction should be closed when the commit is successful or not; note
   *                      the transaction is always closed automatically when a rollback is performed after `f` fails
   * @param f the safe delayed function to attempt before committing or rolling back
   */
  final def attempt[U](retryThreshold: Int, retryDelay: FiniteDuration, closeWhenDone: Boolean = true)(f: => Try[U]): Try[U] = for {
    _      <- attemptOpen()
    result <- f.recoverWith { case error => attemptRollback(error) }
    _      <- Try { attemptCommit(retryThreshold, retryDelay, closeWhenDone) }
  } yield result

  /**
   * Applies `f` on batches of the input `iterator` and then tries to commit the transaction if the result of `f` was a
   * `Success`, or rollback in case of `Failure`. Note that if an error occurs in a batch following other committed
   * batches, on the current batch is rolled back, therefore leaving committed all the previously successful batches.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   * @param closeWhenDone indicates whether the transaction should be closed when the commit is successful or not; note
   *                      the transaction is always closed automatically when a rollback is performed after `f` fails
   * @param f the safe delayed function to attempt before committing or rolling back
   */
  final def attemptBatched[A, U](iterator: Iterator[A], batchSize: Int, retryThreshold: Int, retryDelay: FiniteDuration, closeWhenDone: Boolean = true)(f: Seq[A] => Try[U]): Try[Unit] = Try {
    iterator.grouped(batchSize).foreach { group => attempt[U](retryThreshold, retryDelay, closeWhenDone)(f(group)).get }
  }


  /**
   * Applies `attempt(5, 1.second)(f)`.
   * @param f the safe delayed function to attempt before committing or rolling back
   */
  final def attempt[U](f: => Try[U]): Try[U] = attempt(5, 1.second)(f)

  /**
   * Applies `attemptBatched(iterator)(50, 5, 1.second)(f)`.
   * @param f the safe function to attempt before committing or rolling back
   */
  final def attemptBatched[A, U](iterator: Iterator[A])(f: Seq[A] => Try[U]): Try[Unit] = attemptBatched(iterator, 50, 5, 1.second)(f)

  /**
   * Attempts to commit the current transaction.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   *                       (defaults to 5)
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   *                   (default to 1.second)
   * @param closeWhenDone indicates whether the transaction should be closed when the commit is successful or not
   */
  final def attemptCommit(retryThreshold: Int = 5, retryDelay: FiniteDuration = 1.second, closeWhenDone: Boolean = true) = _attemptCommit(retryThreshold, retryDelay, closeWhenDone, 1)

  @tailrec
  private def _attemptCommit(retryThreshold: Int, retryDelay: FiniteDuration, closeWhenDone: Boolean = true, attempt: Int = 1): Unit = Try { commit() } match {
    case Success(_) if closeWhenDone            =>
      log.info(s"Committed transaction at attempt [$attempt] - closing")
      close()
    case Success(_)                             =>
      log.info(s"Committed transaction at attempt [$attempt] - closing delegated to caller")
    case Failure(e) if attempt > retryThreshold =>
      rollback()
      close()
      throw new RuntimeException(s"Unable to commit transaction after [$attempt] attemp(s) -- rolling back", e)
    case Failure(e)                             =>
      sleepWarning(e, retryDelay + randomDelay) { s"Failed to commit transaction after attempt [$attempt] -- backing off for about [${retryDelay.toSeconds}] second(s)" }
      _attemptCommit(retryThreshold, retryDelay, closeWhenDone, attempt + 1)
  }

}
