package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

import org.cgnal.graphe.application.EnrichedScallop

case class CrossSellApplicationConfig(inputFileLocation: String,
                                      libDir: String,
                                      hadoopDir: String,
                                      tearDown: Boolean,
                                      sparkConfig: SparkApplicationConfig,
                                      securityConfig: SecurityConfig) extends ApplicationConfig

object CrossSellApplicationConfigReader extends ScallopConfigReader[CrossSellApplicationConfig] {

  def scallopts(scallop: Scallop): Scallop =
  { SparkApplicationConfigReader.scallopts _ } andThen
  { SecurityConfigReader.scallopts _         } andThen {
    _.opt[String](name = "input",     short = 'i', descr =  "input file location")
     .opt[String](name = "hadoop",    short = 'h', descr = "hadoop config directory")
     .opt[String](name = "lib-dir",   short = 'l', descr = "extra library directory", default = default("libext"))
     .toggle     (name = "tear-down", short = 'x', descrYes = "tears down the graph and deletes its contents", default = default(false))
  } apply scallop

  protected def consumeScallop(scallop: Scallop): CrossSellApplicationConfig = CrossSellApplicationConfig(
    scallop.mandatory[String]("input"),
    scallop[String]("lib-dir"),
    scallop.mandatory[String]("hadoop"),
    scallop[Boolean]("tear-down"),
    SparkApplicationConfigReader.readScallop(scallop),
    SecurityConfigReader.readScallop(scallop)
  )

}
