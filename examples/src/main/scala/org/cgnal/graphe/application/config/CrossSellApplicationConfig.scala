package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

case class CrossSellApplicationConfig(inputFileLocation: String,
                                      libDir: String,
                                      hadoopDir: String,
                                      tearDown: Boolean,
                                      sparkConfig: SparkApplicationConfig) extends ApplicationConfig

object CrossSellApplicationConfigReader extends ConfigReader[CrossSellApplicationConfig] with ScallopConfigReader[CrossSellApplicationConfig] {

  protected def scallopts(scallop: Scallop): Scallop = scallop
    .opt[String](name = "input",     short = 'i', descr =  "input file location")
    .opt[String](name = "hadoop",    short = 'h', descr = "hadoop config directory")
    .opt[String](name = "lib-dir",   short = 'l', descr = "extra library directory", default = default("libext"))
    .toggle     (name = "tear-down", short = 'x', descrYes = "tears down the graph and deletes its contents", default = default(false))

  protected def consumeScallop(scallop: Scallop): CrossSellApplicationConfig = CrossSellApplicationConfig(
    scallop[String] ("input"),
    scallop[String] ("lib-dir"),
    scallop[String] ("hadoop"),
    scallop[Boolean]("tear-down"),
    SparkApplicationConfigReader.readScallop(scallop)
  )

}
