package org.cgnal.graphe.tinkerpop.titan

import java.util.concurrent.atomic.AtomicBoolean

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.Try

import org.slf4j.LoggerFactory

import com.thinkaurelius.titan.core.{ TitanGraph, TitanTransaction }
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx

import org.cgnal.graphe.tinkerpop.graph.TransactionWrapper

final class TitanTransactionWrapper[A <: TitanTransaction](@transient private[titan] val titanTransaction: A) extends TransactionWrapper with Serializable {

  def commit(): Unit = withDebug("Committing transaction") { titanTransaction.commit() }

  def rollback(): Unit = withDebug("Rolling back transaction") { titanTransaction.rollback() }

  def isOpen: Boolean = titanTransaction.isOpen

  def open(): Unit = log.debug("Transaction cannot be opened")

  def close(): Unit = withDebug("Closing Transaction") { titanTransaction.close() }

  /**
   * Attempts to execute a function on the wrapped Titan transaction type.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   * @param closeWhenDone indicates whether the transaction should be closed when the commit is successful or not; note
   *                      the transaction is always closed automatically when a rollback is performed after `f` fails
   * @param f the safe delayed function to attempt before committing or rolling back
   */
  def attemptTitanTransaction[U](retryThreshold: Int, retryDelay: FiniteDuration, closeWhenDone: Boolean = true)(f: A => Try[U]): Try[U] = TitanTransactionLock.acquire(retryThreshold, retryDelay) {
    attempt(retryThreshold, retryDelay, closeWhenDone) { f(titanTransaction) }
  }

}

object TitanTransactionWrapper {

  def create(graph: TitanGraph, batchMode: Boolean = false) = if (batchMode) batch(graph) else standard(graph)

  def standard(graph: TitanGraph) = new TitanTransactionWrapper(graph.newTransaction().asInstanceOf[StandardTitanTx])

  def batch(graph: TitanGraph) = new TitanTransactionWrapper(
    graph.asInstanceOf[StandardTitanGraph].buildTransaction()
      .threadBound()
      .consistencyChecks(false)
      .checkInternalVertexExistence(false)
      .checkExternalVertexExistence(false)
      .start()
      .asInstanceOf[StandardTitanTx]
  )

  def withIdManager[U](transaction: StandardTitanTx)(f: IDManager => U) = Try { f(transaction.getIdInspector) }

  def batched[A](graph: TitanGraph, iterator: Iterator[A], batchSize: Int, retryThreshold: Int, retryDelay: FiniteDuration, batchMode: Boolean = false)(f: (Seq[A], StandardTitanTx) => Try[Unit]) = Try {
    iterator.grouped(batchSize).foreach { batch =>
      create(graph, batchMode).attemptTitanTransaction(retryThreshold, retryDelay) { f(batch, _) }.get
    }
  }

}

private[titan] object TitanTransactionLock {

  @transient private lazy val log = LoggerFactory.getLogger("cgnal.titan.Lock")

  private var locked: AtomicBoolean = new AtomicBoolean(false)

  private def waitLockout(attemptNumber: Int, retryDelay: FiniteDuration) = {
    log.warn(s"Unable obtain lock after [$attemptNumber] attempt(s); backing-off for [${retryDelay.toMillis}] millisecond(s)")
    Thread.sleep(retryDelay.toMillis)
  }

  private def withLockout[A](f: => A): A = synchronized {
    val result = Try { f }
    locked.set(false)
    result.get
  }

  @tailrec
  private def _acquire[A](retryThreshold: Int, retryDelay: FiniteDuration, attemptNumber: Int = 1)(f: => A): A = synchronized {
    if      (locked.compareAndSet(false, true)) withLockout { f }
    else if (attemptNumber > retryThreshold) throw new RuntimeException(s"Unable to acquire lock after [$attemptNumber] attempt(s)")
    else _acquire(retryThreshold, retryDelay, attemptNumber + 1)(f)
  }

  def acquire[A](retryThreshold: Int, retryDelay: FiniteDuration)(f: => A) = _acquire(retryThreshold, retryDelay)(f)

}


