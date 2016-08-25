package org.cgnal.graphe.tinkerpop.titan

import scala.collection.convert.decorateAsScala._

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import com.thinkaurelius.titan.diskstorage.configuration.{ ConfigElement, ConfigNamespace }
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration

import org.cgnal.graphe.tinkerpop.config.ResourceConfig
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

trait TitanResourceConfig extends ResourceConfig { this: TinkerGraphProvider =>

  protected def namespaceMappings = Seq(
    GraphDatabaseConfiguration.STORAGE_NS -> TitanHadoopConfiguration.GRAPH_CONFIG_KEYS -> false
  )

  private def copyMapReduce(base: TinkerConfig, sourceNamespace: ConfigNamespace, targetNamespace: ConfigNamespace, includeRoot: Boolean) =
    base.getKeys(ConfigElement.getPath(sourceNamespace, includeRoot)).asScala.toList.foreach { key =>
      base.setProperty(
        s"${ConfigElement.getPath(targetNamespace, true)}.$key",
        base.getProperty(key))
    }

  private def copyAllMapReduce(base: TinkerConfig) = {
    namespaceMappings.foreach { case ((source, target), includeRoot) => copyMapReduce(base, source, target, includeRoot) }
    base
  }

  override def config = copyAllMapReduce { super.config }

}
