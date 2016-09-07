package org.cgnal.graphe.tinkerpop.graph

import org.cgnal.graphe.tinkerpop.graph.helper.TransactionWrapperCases

class TransactionWrapperTest extends TransactionWrapperCases {

  "Transaction Wrapper" when {

    "trying to commit" must {

      "commit successfully" in successfulCommit()

      "commit after failing once" in failOnce()

      "commit after retrying multiple times" in failMultipleTimes()

      "rollback after failing a sufficiently large number of times" in failTooManyTimes()

    }

    "working with transactional operations" must {

      "commit successfully when the operation is a Success" in successfulTransaction()

      "rollback successfully when the operation is a Failure" in failedTransaction()

    }

    "working with batched operations" must {

      "commit successfully when the operations are Successful" in successfulBatchedTransaction()

      "rollback successfully when a batch operation is a Failure" in failedBatchedTransaction()

    }

  }

}
