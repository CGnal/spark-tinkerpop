package org.cgnal.graphe.tinkerpop.config

import java.io.{ InputStream, InputStreamReader }

import scala.util.Properties

import org.apache.commons.configuration.{ Configuration => TinkerConfig, XMLConfiguration, FileConfiguration => TinkerFileConfig, BaseConfiguration }
import org.apache.commons.configuration.plist.PropertyListConfiguration

import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration

import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

/**
 * Reads the graph configuration file and implements the `TinkerGraphProvider`'s `config` function. The loading
 * mechanism tries to first load the file specified by the system property `-Dcgnal.graph.config` and then simply tries
 * to load a file with name `graph=config.yml`. Note that the configuration file is always assumed to be on the
 * classpath, and should be of type `yaml`, `yml`, `xml` or `properties`.
 */
trait ResourceConfig { this: TinkerGraphProvider =>

  /**
   * The default confguration file name (defaults to `graph-config.yml`).
   */
  protected def defaultConfigFileName = "graph-config.yml"

  /**
   * The system property name that contains the name of the configuration file (defaults to `cgnal.graph.config`). Note
   * that the file should be on the classpath and be of type `yaml`, `yml`, `xml` or `properties`.
   */
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