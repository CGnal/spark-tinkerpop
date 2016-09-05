package org.cgnal.graphe.tinkerpop.graph

import scala.concurrent.duration._
import scala.util.Try

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph }

import org.cgnal.graphe.tinkerpop.config._

/**
 * Facade for convenience methods on a tinkerpop graph. Provides useful closure functions that expose the underlying
 * graph and any transaction needed by the client. Note that the provider is also expected to be responsible for the
 * graph initialization and configuration.
 */
trait TinkerGraphProvider { this: Serializable =>

  /**
   * The amount of times a transaction commit should be retried in case of failure (defaults to `5`)
   */
  protected def retryThreshold = config.getInt(retryKey, retryValue)

  /**
   * The amount of time to wait in `FiniteDuration` before reattempting a commit on a transaction in case of failure
   * (defaults to `1.5 seconds`)
   */
  protected def retryDelay = config.getInt(backOffKey, backOffValue).milliseconds

  /**
   * The default amount of elements to process in an iterator before attempting a commit (defaults to `100`).
   */
  protected def defaultBatchSize = config.getInt(batchSizeKey, batchSizeValue)

  /**
   * The configuration for this graph.
   */
  protected def config: TinkerConfig

  /**
   * The underlying tinkerpop `Graph`.
   */
  protected def graph: TinkerGraph

  /**
   * Factory sort of method that creates a wrapped transaction that exposes commit attempt behavior.
   */
  protected def createTransaction: TransactionWrapper

  /**
   * Publicly exposes the graph configruration. This method may be overridden to hide some aspects of confguration if
   * necessary.
   */
  def tinkerConfig = config

  /**
   * Provides a closure for accessing the internal `Graph` instance; note that the graph is not wrapped or shadowed in
   * any way, assuming that the client is '''NOT''' expected to perform any management or mutation of the graph instance
   * itself. The operation is assumed to not require a transaction, usually useful when reading.
   * @param f the function to apply on the internal graph instance
   * @return the result of applying `f` on the graph
   */
  def withGraph[U](f: TinkerGraph => U): U = f { graph }

  /**
   * Provides a closure for accessing the internal graph instance, together with a transaction. Note that the
   * transaction can be assumed to be open and should not be committed or closed by the caller in that responsibility
   * is delegated to the `TransactionWrapper`, which will attempt committing at the end of successful execution of the
   * enclosed function `f` (or rollback in case of failure) based on the retry strategy parameters provided.
   * @param f the function to apply on the internal graph instance and the transaction
   * @return the `Try` result of applying `f` on the graph and transaction
   */
  def withGraphTransaction[U](f: (TinkerGraph, TransactionWrapper) => U): Try[U] = createTransaction.attemptTransaction(retryThreshold, retryDelay) { transaction =>
    Try { f(graph, transaction) }
  }

  /**
   * Provides a closure for accessing the internal graph instance, without needing direct access to a transaction, even
   * though `f` is assumed to require a commit/rollback strategy. The behavior here is therefore identical to
   * `withGraphTransaction` but without exposing the transaction to the caller.
   * @param f the function to apply on the internal graph instance
   * @return the `Try` result of applying `f` on the graph
   */
  def withGraphTransaction[U](f: TinkerGraph => U): Try[U] = withGraphTransaction { (g, _) => f(g) }

  /**
   * Consumes an iterator, exposing a transaction to a function that is applied on each element of the iterator,
   * expecting the function to side-effect on the iterator. The transaction is committed depending on the setting
   * provided for `batchSize`, which defaults to the of `defaultBatchSize` provided by the implementing class (default
   * 100).
   * @param iterator the `Iterator` over which the function `f` is applied
   * @param batchSize specifies how often the transaction is committed in terms of the number of iterations
   * @param f the function tp apply on the internal graph and each element of `iterator`
   */
  def withGraphTransaction[A](iterator: Iterator[A], batchSize: Int = defaultBatchSize)(f: (TinkerGraph, A) => Unit): Try[Unit] = withGraphTransaction[Unit] { (graph: TinkerGraph, transaction: TransactionWrapper) =>
    iterator.zipWithIndex.foreach { case (a, index) =>
      f(graph, a)
      if ((index % batchSize) == 0) transaction.attemptCommit(retryThreshold, retryDelay, false)
    }
    transaction.attemptCommit(retryThreshold, retryDelay, true)
  }

}
