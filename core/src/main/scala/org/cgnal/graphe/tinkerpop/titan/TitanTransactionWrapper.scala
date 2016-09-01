package org.cgnal.graphe.tinkerpop.titan

import scala.util.Try

import scala.concurrent.duration._

import com.thinkaurelius.titan.core.{ TitanGraph, TitanTransaction }
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx

import org.cgnal.graphe.tinkerpop.graph.TransactionWrapper

final class TitanTransactionWrapper[A <: TitanTransaction](@transient private val titanTransaction: A) extends TransactionWrapper with Serializable {

  def commit(): Unit = withDebug("Committing transaction") { titanTransaction.commit() }

  def rollback(): Unit = withDebug("Rolling back transaction") { titanTransaction.rollback() }

  def isOpen: Boolean = titanTransaction.isOpen

  def open(): Unit = log.debug("Transaction cannot be opened")

  def close(): Unit = withDebug("Closing Transaction") { titanTransaction.close() }

  /**
   * Attempts to execute a function on the wrapped Titan transaction type.
   * @param retryThreshold the amount of times to reattempt a commit before declaring failure and rolling back
   * @param retryDelay the amount of time to back-off the current thread before reattempting to commit the transaction
   * @param f the safe delayed function to attempt before committing or rolling back
   */
  def attemptTitanTransaction[U](retryThreshold: Int, retryDelay: FiniteDuration)(f: A => Try[U]): Try[U] = attempt(retryThreshold, retryDelay) { f(titanTransaction) }

}

object TitanTransactionWrapper {

  def create(graph: TitanGraph) = new TitanTransactionWrapper(graph.newTransaction())

  def standard(graph: TitanGraph) = new TitanTransactionWrapper(graph.newTransaction().asInstanceOf[StandardTitanTx])

  def withIdManager[U](transaction: StandardTitanTx)(f: IDManager => U) = Try { f(transaction.getIdInspector) }

  def batched[A](graph: TitanGraph, iterator: Iterator[A], batchSize: Int, retryThreshold: Int, retryDelay: FiniteDuration)(f: (Seq[A], StandardTitanTx) => Try[Unit]) = Try {
    iterator.grouped(batchSize).foreach { batch =>
      standard(graph).attemptTitanTransaction(retryThreshold, retryDelay) { f(batch, _) }.get
    }
  }

}


