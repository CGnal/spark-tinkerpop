package org.cgnal.graphe.tinkerpop.graph.helper

import org.cgnal.graphe.tinkerpop.graph.TransactionWrapper

sealed class TestTransactionWrapper(private var expectedFails: Int = 0) extends TransactionWrapper with Serializable {

  private var isTransactionOpen = false

  private var _transactionLog = List.empty[Action]

  private def logTransaction(action: Action) = _transactionLog = { action :: _transactionLog }

  def commit()   =
    if (expectedFails <= 0) logTransaction { Action.commit() }
    else {
      logTransaction { Action.commit() }
      expectedFails = expectedFails - 1
      throw new RuntimeException("Transaction was set up to fail")
    }

  def rollback() = logTransaction { Action.rollback() }

  def open() = isTransactionOpen = true

  def close() = isTransactionOpen = false

  def isOpen = isTransactionOpen

  def lastTransaction = _transactionLog.head

  def transactionLog = _transactionLog

  def numCommits = transactionLog.count { _.action == Action.committed }

  def numRollbacks = transactionLog.count { _.action == Action.rolledBack }

  def hasRolledBack = numRollbacks > 0

  def timeElapsed = transactionLog match {
    case action1 :: action2 :: _ => { action1.timestamp - action2.timestamp }.toInt
    case other                   => throw new RuntimeException(s"Transaction log must contain at least 2 entries, found [${other.size}]")
  }

}

object TestTransactionWrapper {

  def create                 = new TestTransactionWrapper(0)

  def failing(numFails: Int) = new TestTransactionWrapper(numFails)

}

sealed case class Action(timestamp: Long, action: String)

private[helper] object Action {

  def committed  = "committed"

  def rolledBack = "rolledBack"

  private def now() = System.currentTimeMillis()

  def commit()   = apply(now(), committed)

  def rollback() = apply(now(), rolledBack)

}
