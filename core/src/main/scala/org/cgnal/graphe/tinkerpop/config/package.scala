package org.cgnal.graphe.tinkerpop

package object config {

  val backOffKey   = "application.back-off-time"
  val backOffValue = 1500

  val retryKey   = "application.retries"
  val retryValue = 5

  val batchSizeKey   = "application.batch-size"
  val batchSizeValue = 100

}
