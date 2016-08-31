package org.cgnal.graphe.application.config

import org.rogach.scallop.Scallop

case class CrossSellApplicationConfig(inputFileLocation: String,
                                      libDir: String,
                                      hadoopDir: String,
                                      sparkConfig: SparkApplicationConfig) extends ApplicationConfig

object CrossSellApplicationConfigReader extends ConfigReader[CrossSellApplicationConfig] with ScallopConfigReader[CrossSellApplicationConfig] {

  protected def scallopts(scallop: Scallop): Scallop = scallop
    .opt[String]("input",  'i', "input file location")
    .opt[String]("hadoop", 'h', "hadoop config directory")
    .opt[String]("libDir", 'l', "extra library directory", default("libext"))

  protected def consumeScallop(scallop: Scallop): CrossSellApplicationConfig = CrossSellApplicationConfig(
    scallop[String]("input"),
    scallop[String]("libDir"),
    scallop[String]("hadoop"),
    SparkApplicationConfigReader.readScallop(scallop)
  )

}
