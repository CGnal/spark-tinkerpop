package org.cgnal.graphe.tinkerpop.graph.titan

import scala.util.Try

import scala.concurrent.duration._

import com.thinkaurelius.titan.core.{ TitanGraph, TitanTransaction }
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx

import org.cgnal.graphe.tinkerpop.graph.TransactionWrapper

class TitanTransactionWrapper[A <: TitanTransaction](@transient private val titanTransaction: A) extends TransactionWrapper with Serializable {

  def commit(): Unit = withDebug("Committing transaction") { titanTransaction.commit() }

  def rollback(): Unit = withDebug("Rolling back transaction") { titanTransaction.rollback() }

  def isOpen: Boolean = titanTransaction.isOpen

  def open(): Unit = log.debug("Transaction already open")

  def close(): Unit = withDebug("Closing Transaction") { titanTransaction.close() }

  def attemptTitanTransaction[U](retryThreshold: Int, retryDelay: FiniteDuration)(f: A => Try[U]): Try[U] = attempt(retryThreshold, retryDelay) { f(titanTransaction) }

}

object TitanTransactionWrapper {

  def create(graph: TitanGraph) = new TitanTransactionWrapper(graph.newTransaction())

  def standard(graph: TitanGraph) = new TitanTransactionWrapper(graph.newTransaction().asInstanceOf[StandardTitanTx])

  def withIdManager[U](transaction: StandardTitanTx)(f: (IDManager, StandardTitanTx) => U) = Try { f(transaction.getIdInspector, transaction) }

}


