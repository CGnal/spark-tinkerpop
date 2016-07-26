package org.cgnal.graphe.tinkerpop.graph

import scala.util.Try
import scala.collection.convert.decorateAsScala._

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.tinkerpop.gremlin.structure.{ Graph => TinkerGraph, Transaction => TinkerTransaction }

trait TinkerGraphProvider { this: Serializable =>

  protected def config: TinkerConfig

  protected def graph: TinkerGraph

  def tinkerConfig = config

  def withGraph[U](f: TinkerGraph => U): U = f { graph }

  def withGraphTransaction[U](f: (TinkerGraph, TinkerTransaction) => U): Try[U] = {
    val transaction = graph.tx()
    if (!transaction.isOpen) transaction.open()
    val u = Try { f(graph, transaction) }
    transaction.commit()
    transaction.close()
    u
  }

  def withGraphTransaction[U](f: TinkerGraph => U): Try[U] = withGraphTransaction { (g, _) => f(g) }

  def withGraphTransaction[A](iterator: Iterator[A])(f: (TinkerGraph, A) => Unit): Try[Unit] = withGraphTransaction[Unit] { graph: TinkerGraph =>
    iterator.foreach { f(graph, _) }
  }

}
