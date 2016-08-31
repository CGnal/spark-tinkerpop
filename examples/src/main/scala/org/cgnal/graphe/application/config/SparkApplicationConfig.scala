package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

case class SparkApplicationConfig(numThreads: Int,
                                  executorMemory: String,
                                  numPartitions: Int,
                                  shuffleFraction: Double,
                                  storageFraction: Double) extends Config

object SparkApplicationConfigReader extends ConfigReader[SparkApplicationConfig] with ScallopConfigReader[SparkApplicationConfig] {

  protected def scallopts(scallop: Scallop): Scallop = scallop
  .opt[Int]   ("threads",    't', "number of executor threads", default(2))
  .opt[String]("memory",     'm', "executor memory",            default("2g"))
  .opt[Int]   ("partitions", 'p', "default num partitions",     default(2))
  .opt[Double]("shuffle",    'x', "shuffle fraction",           default(0.1))
  .opt[Double]("storage",    's', "storage fraction",           default(0.4))

  protected def consumeScallop(scallop: Scallop): SparkApplicationConfig = SparkApplicationConfig(
    scallop[Int]   ("threads"),
    scallop[String]("memory"),
    scallop[Int]   ("partitions"),
    scallop[Double]("shuffle"),
    scallop[Double]("storage")
  )

}
