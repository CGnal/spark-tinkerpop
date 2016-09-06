package org.cgnal.graphe.tinkerpop.config.helper

import scala.util.{Properties, Try}

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.cgnal.graphe.tinkerpop.config.ResourceConfig
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

sealed class TestResourceConfig extends ResourceConfig with TinkerGraphProvider with Serializable {

  final protected def graph = throw new RuntimeException("No graph defined in test")

  final protected def createTransaction = throw new RuntimeException("No transaction defined in test")

  final def internalConfig = config

  final def fileKey = configFileKey

}

trait ResourceConfigHelper {

  final def withConfig(name: String)(f: TinkerConfig => Unit): Unit = {
    val testConfig = new TestResourceConfig
    System.getProperties.put(testConfig.fileKey, name)
    val result = Try { f(testConfig.internalConfig) }
    System.getProperties.remove(testConfig.fileKey)
    result.get
  }

  final def withConfig(f: TinkerConfig => Unit) = f(new TestResourceConfig().internalConfig)

}
