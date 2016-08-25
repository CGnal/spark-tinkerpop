package org.cgnal.graphe.tinkerpop.config

import java.io.{ InputStream, InputStreamReader }

import org.apache.commons.configuration.{ Configuration => TinkerConfig, XMLConfiguration, FileConfiguration => TinkerFileConfig, BaseConfiguration }
import org.apache.commons.configuration.plist.PropertyListConfiguration

import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration

import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

import scala.util.Properties

trait ResourceConfig { this: TinkerGraphProvider =>

  protected def defaultConfigFileName = "graph-config.yml"

  protected def configFileKey = "cgnal.graph.config"

  private lazy val configFileName = Properties.propOrElse(configFileKey, defaultConfigFileName)

  private lazy val lowerConfigFileName = configFileName.toLowerCase

  private def withInputStream[A](f: InputStream => A) = {
    val stream = this.getClass.getClassLoader.getResourceAsStream { configFileName }
    if (stream == null) throw new RuntimeException(s"Unable to find resource [$configFileName]; make sure it exists and that it is available on the classpath")
    else f { stream }
  }

  private def asReader = withInputStream { new InputStreamReader(_) }

  private def chooseConfigReader: TinkerFileConfig =
    if      (lowerConfigFileName endsWith "yml")        new YamlConfiguration()
    else if (lowerConfigFileName endsWith "yaml")       new YamlConfiguration()
    else if (lowerConfigFileName endsWith "xml")        new XMLConfiguration()
    else if (lowerConfigFileName endsWith "properties") new PropertyListConfiguration()
    else throw new IllegalArgumentException("Configuration file name has incompatible type; must be one of [yml | yaml | xml | properties]")

  private def loadConfig(configuration: TinkerFileConfig) = {
    configuration.load(asReader)
    val base = new BaseConfiguration()
    base.copy(configuration)
    base.clearProperty { "" }
    base
  }

  protected def config: TinkerConfig = loadConfig { chooseConfigReader }

}