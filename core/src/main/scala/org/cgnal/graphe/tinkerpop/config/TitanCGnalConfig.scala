package org.cgnal.graphe.tinkerpop.config

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption
import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption.{ Type => ConfigType }
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration

object TitanCGnalConfig {

  val VERTEX_QUERY = new ConfigOption(
    TitanHadoopConfiguration.IOFORMAT_NS,
    "vertex-query",
    "Vertex Query in Gremlin language which is run as pre-filtering step before while loading",
    ConfigType.LOCAL,
    classOf[String],
    "v.query()")

}
