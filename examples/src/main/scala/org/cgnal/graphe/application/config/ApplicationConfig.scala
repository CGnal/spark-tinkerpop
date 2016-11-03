package org.cgnal.graphe.application.config

trait ApplicationConfig extends Config {

  def isStandalone: Boolean = sparkConfig.numThreads > 0

  def isSecured: Boolean = securityConfig != NoSecurityConfig

  def hadoopDir: String

  def libDir: String

  def securityConfig: SecurityConfig

  def sparkConfig: SparkApplicationConfig

}