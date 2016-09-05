package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

trait ApplicationConfig extends Config {

  def isStandalone: Boolean = sparkConfig.numThreads > 0

  def hadoopDir: String

  def libDir: String

  def securityConfig: SecurityConfig

  def sparkConfig: SparkApplicationConfig

}