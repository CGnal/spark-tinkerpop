package org.cgnal.graphe.tinkerpop.titan

import scala.collection.convert.decorateAsScala._

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import com.thinkaurelius.titan.diskstorage.configuration.{ ConfigElement, ConfigNamespace }
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration

import org.cgnal.graphe.tinkerpop.config.ResourceConfig
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider

/**
 * Extends the default `ResourceConfig` trait to allow the seamless copying of specific prefixed elements onto others,
 * which avoids repetition across for example `storage` and `titanmr.io.conf` elements in the configuration file itself.
 */
trait TitanResourceConfig extends ResourceConfig { this: TinkerGraphProvider =>

  /**
   * The source and target namespaces in the form of a `Seq[((ConfigNamespace, ConfigNamespace), Boolean)]`, which
   * represents `((source, target), includeSourceRoot)`. The result is that all config elements with prefix
   * `root.${source}` (or simply `${source}` if `includeSourceRoot` is set to `false`) will be copied onto the target
   * namespace as `${target}.${source}`, for example
   * {{{
   *   storage.backend = hbase*
   * }}}
   * becomes effectively
   * {{{
   *   storage.backend = hbase
   *   titanmr.io.conf.storage.backend = hbase
   * }}}
   *
   * Note that the default value for this is simply
   * {{{
   *   Seq { GraphDatabaseConfiguration.STORAGE_NS -> TitanHadoopConfiguration.GRAPH_CONFIG_KEYS -> false }
   * }}}
   * which copies all `storage` elements in configuration onto the `titanmr.io.conf` namespace.
   */
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
