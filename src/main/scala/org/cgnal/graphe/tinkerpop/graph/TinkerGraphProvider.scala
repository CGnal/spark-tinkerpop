package org.cgnal.graphe.tinkerpop.graph

import scala.concurrent.duration._
import scala.util.Try

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph }

trait TinkerGraphProvider { this: Serializable =>

  protected def retryThreshold = 5

  protected def retryDelay = 1.second

  protected def defaultBatchSize = 100

  protected def config: TinkerConfig

  protected def graph: TinkerGraph

  protected def createTransaction: TransactionWrapper


  def tinkerConfig = config

  def withGraph[U](f: TinkerGraph => U): U = f { graph }

  def withGraphTransaction[U](f: (TinkerGraph, TransactionWrapper) => U): Try[U] = createTransaction.attemptTransaction(retryThreshold, retryDelay) { transaction =>
    Try { f(graph, transaction) }
  }

  def withGraphTransaction[U](f: TinkerGraph => U): Try[U] = withGraphTransaction { (g, _) => f(g) }

  def withGraphTransaction[A](iterator: Iterator[A], batchSize: Int = defaultBatchSize)(f: (TinkerGraph, A) => Unit): Try[Unit] = withGraphTransaction[Unit] { (graph: TinkerGraph, transaction: TransactionWrapper) =>
    iterator.zipWithIndex.foreach { case (a, index) =>
      f(graph, a)
      if ((index % batchSize) == 0) transaction.attemptCommit(retryThreshold, retryDelay)
    }
  }

}
