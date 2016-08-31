package org.cgnal.graphe.tinkerpop.graph

import java.util.function.Consumer

import scala.collection.mutable.ListBuffer

import org.apache.tinkerpop.gremlin.structure.Transaction.Status
import org.apache.tinkerpop.gremlin.structure.util.AbstractTransaction
import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Transaction }

/**
 * A transaction implementation that does nothing (used only internally by the `EmptyTinkerGraph`.
 * @param graph the owning graph
 */
final class EmptyTransaction(graph: TinkerGraph) extends AbstractTransaction(graph) {

  private def doNothing: Unit = ()

  private var transactionState = true

  private var statusListeners = ListBuffer.empty[Consumer[Status]]

  def isOpen = transactionState

  def doReadWrite() = doNothing

  def doCommit() = doNothing

  def doRollback() = doNothing

  def fireOnCommit() = statusListeners.foreach { _.accept(Status.COMMIT) }

  def fireOnRollback() = statusListeners.foreach { _.accept(Status.ROLLBACK) }

  def doOpen() = transactionState = true

  def doClose() = transactionState = false

  def onReadWrite(consumer: Consumer[Transaction]) = {
    consumer.accept(this)
    this
  }

  def onClose(consumer: Consumer[Transaction]) = {
    consumer.accept(this)
    this
  }

  def addTransactionListener(listener: Consumer[Status]) = statusListeners += listener

  def clearTransactionListeners() = statusListeners = ListBuffer.empty[Consumer[Status]]

  def removeTransactionListener(listener: Consumer[Status]) = statusListeners -= listener

}
