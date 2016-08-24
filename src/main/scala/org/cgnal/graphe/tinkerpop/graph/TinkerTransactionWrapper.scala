package org.cgnal.graphe.tinkerpop.graph

import org.apache.tinkerpop.gremlin.structure.{ Transaction => TinkerTransaction, Graph => TinkerGraph }

class TinkerTransactionWrapper(@transient private val tinkerTransaction: TinkerTransaction) extends TransactionWrapper with Serializable {

  def commit(): Unit = tinkerTransaction.commit()

  def rollback(): Unit = tinkerTransaction.rollback()

  def isOpen: Boolean = tinkerTransaction.isOpen

  def open(): Unit = tinkerTransaction.open()

  def close(): Unit = tinkerTransaction.close()

}

object TinkerTransactionWrapper {

  def create(graph: TinkerGraph) = new TinkerTransactionWrapper(graph.tx())

}
