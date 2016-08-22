package org.cgnal.graphe.tinkerpop.graph

import scala.annotation.tailrec
import scala.concurrent.duration._
import scala.util.{ Try, Success, Failure }

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.slf4j.LoggerFactory

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Transaction => TinkerTransaction }

trait TinkerGraphProvider { this: Serializable =>

  @transient protected lazy val log = LoggerFactory.getLogger(s"graph.provider.${this.getClass.getSimpleName.filter { _.isLetterOrDigit }}")

  protected def retryThreshold = 5

  protected def retryDelay = 1.second

  protected def defaultBatchSize = 100

  protected def config: TinkerConfig

  protected def graph: TinkerGraph

  final protected def sleepWarning(m: String) = {
    log.warn(m)
    Thread.sleep(retryDelay.toMillis)
  }

  @tailrec
  final protected def tryCommit(transaction: TinkerTransaction, attempt: Int = 1): Unit = Try { transaction.commit() } match {
    case Success(_)                             => log.info(s"Committed transaction at attempt [$attempt] - closing"); transaction.close()
    case Failure(e) if attempt > retryThreshold => transaction.rollback(); transaction.close(); throw new RuntimeException(s"Unable to commit transaction after [$attempt] attemp(s) -- rolling back", e)
    case Failure(_)                             => sleepWarning { s"Failed to commit transaction after attempt [$attempt] -- backing off for [${retryDelay.toSeconds}] second(s)" }; tryCommit(transaction, attempt + 1)
  }

  def tinkerConfig = config

  def withGraph[U](f: TinkerGraph => U): U = f { graph }

  def withGraphTransaction[U](f: (TinkerGraph, TinkerTransaction) => U): Try[U] = {
    val transaction = graph.tx()
    if (!transaction.isOpen) transaction.open()
    val u = Try { f(graph, transaction) }
    tryCommit(transaction)
    u
  }

  def withGraphTransaction[U](f: TinkerGraph => U): Try[U] = withGraphTransaction { (g, _) => f(g) }

  def withGraphTransaction[A](iterator: Iterator[A], batchSize: Int = defaultBatchSize)(f: (TinkerGraph, A) => Unit): Try[Unit] = withGraphTransaction[Unit] { (graph: TinkerGraph, transaction: TinkerTransaction) =>
    iterator.zipWithIndex.foreach { case (a, index) =>
      f(graph, a)
      if ((index % batchSize) == 0) tryCommit(transaction)
    }
  }

}
