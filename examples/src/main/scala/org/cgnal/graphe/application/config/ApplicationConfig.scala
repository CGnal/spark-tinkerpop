package org.cgnal.graphe.application.config

trait ApplicationConfig extends Config {

  def isStandalone: Boolean = sparkConfig.numThreads > 0

  def hadoopDir: String

  def libDir: String

  def sparkConfig: SparkApplicationConfig

}
