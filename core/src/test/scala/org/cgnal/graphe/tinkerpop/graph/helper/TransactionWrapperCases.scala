package org.cgnal.graphe.tinkerpop.graph.helper

import scala.concurrent.duration._
import scala.util.{ Success, Failure, Try }

import org.scalatest.{ MustMatchers, WordSpec }

trait TransactionWrapperCases extends WordSpec with MustMatchers {

  private def failure = Failure { new RuntimeException("Expected exception in test") }

  private def success = Success ()

  private def test[A](f: A => Unit): A => Try[Unit] = { a => Try(f(a)) }

  private def testBoolean[A](f: A => Boolean): A => Try[Unit] = { a =>
    if (f(a)) success
    else      failure
  }

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
    wrapper.timeElapsed            mustBe >= (1000)
  }

  protected def failMultipleTimes() = {
    val wrapper = TestTransactionWrapper.failing(4)
    wrapper.attemptCommit(retryThreshold = 5, retryDelay = 200.milliseconds)

    wrapper.transactionLog.size    mustBe 5
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
    wrapper.timeElapsed            mustBe >= (200)
  }

  protected def failTooManyTimes() = {
    val wrapper = TestTransactionWrapper.failing(10)
    a [RuntimeException] should be thrownBy wrapper.attemptCommit(retryThreshold = 2, retryDelay = 200.milliseconds)

    wrapper.lastTransaction.action mustBe Action.rolledBack
    wrapper.timeElapsed            mustBe <= (50)
    wrapper.transactionLog.size    mustBe 4
  }

  protected def successfulTransaction() = {
    val wrapper = TestTransactionWrapper.create
    wrapper.attemptTransaction(retryThreshold = 5, retryDelay = 200.milliseconds) { _ => success }

    wrapper.transactionLog.size    mustBe 1
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
  }

  protected def failedTransaction() = {
    val wrapper = TestTransactionWrapper.create
    wrapper.attemptTransaction(retryThreshold = 5, retryDelay = 200.milliseconds) { _ => failure }

    wrapper.transactionLog.size    mustBe 1
    wrapper.lastTransaction.action mustBe Action.rolledBack
    wrapper.hasRolledBack          mustBe true
  }

  protected def successfulBatchedTransaction() = {
    val wrapper  = TestTransactionWrapper.create
    val iterator = Iterator.range(0, 20)

    wrapper.attemptBatched(iterator, batchSize = 5, retryThreshold = 5, retryDelay = 200.milliseconds) {
      test { _.size mustBe 5 }
    }

    wrapper.transactionLog.size    mustBe 4
    wrapper.lastTransaction.action mustBe Action.committed
    wrapper.hasRolledBack          mustBe false
  }

  protected def failedBatchedTransaction() = {
    val wrapper  = TestTransactionWrapper.create
    val iterator = Iterator.range(0, 20)

    wrapper.attemptBatched(iterator, batchSize = 5, retryThreshold = 5, retryDelay = 200.milliseconds) {
      testBoolean { _.head < 5 }
    }

    wrapper.transactionLog.size    mustBe 2
    wrapper.lastTransaction.action mustBe Action.rolledBack
    wrapper.timeElapsed            mustBe < (200)
    wrapper.numCommits             mustBe 1
    wrapper.hasRolledBack          mustBe true
  }

}
