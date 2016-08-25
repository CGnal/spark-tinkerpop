package org.cgnal.graphe.tinkerpop.graph

import org.apache.commons.configuration.BaseConfiguration

object EmptyTinkerGraphProvider extends TinkerGraphProvider with Serializable {

  @transient val config = new BaseConfiguration

  protected val graph = new EmptyTinkerGraph(config)

  protected def createTransaction: TransactionWrapper = TinkerTransactionWrapper.create(graph)

}