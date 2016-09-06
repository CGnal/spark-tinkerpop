package org.cgnal.graphe.tinkerpop.graph

import org.cgnal.graphe.tinkerpop.graph.helper.TransactionWrapperCases

class TransactionWrapperTest extends TransactionWrapperCases {

  "Transaction Wrapper" when {

    "working with successful operations" must {

      "commit successfully" in successfulCommit()

    }

    "working with unsuccessful operations" must {

      "commit after failing once" in failOnce()

      "commit after retrying multiple times" in failMultipleTimes()

      "rollback after failing a sufficiently large number of times" in rollbackAfterFailure()

    }

  }

}
