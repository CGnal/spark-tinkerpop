package org.cgnal.graphe.tinkerpop.graph.helper

import scala.concurrent.duration._

import org.scalatest.{ MustMatchers, WordSpec }

trait TransactionWrapperCases extends WordSpec with MustMatchers {

  protected def successfulCommit() = {
    val wrapper = TestTransactionWrapper.create
    wrapper.attemptCommit()

    wrapper.transactionLog.size    mustBe 1
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
  }

  protected def failOnce() = {
    val wrapper = TestTransactionWrapper.failing(1)
    wrapper.attemptCommit()

    wrapper.transactionLog.size    mustBe 2
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
    wrapper.timeElapsed.toInt      mustBe >= (1000)
  }

  protected def failMultipleTimes() = {
    val wrapper = TestTransactionWrapper.failing(4)
    wrapper.attemptCommit(retryThreshold = 5, retryDelay = 200.milliseconds)

    wrapper.transactionLog.size    mustBe 5
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
    wrapper.timeElapsed.toInt      mustBe >= (200)
  }

  protected def rollbackAfterFailure() = {
    val wrapper = TestTransactionWrapper.failing(10)
    a [RuntimeException] should be thrownBy wrapper.attemptCommit(retryThreshold = 2, retryDelay = 200.milliseconds)

    wrapper.lastTransaction.action mustBe Action.rolledBack
    wrapper.timeElapsed.toInt      mustBe <= (50)
    wrapper.transactionLog.size    mustBe 4
  }

}
