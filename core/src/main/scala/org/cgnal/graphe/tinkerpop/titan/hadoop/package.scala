package org.cgnal.graphe.tinkerpop.titan

import com.thinkaurelius.titan.diskstorage.configuration.ConfigOption.{ Type => ConfigType }
import com.thinkaurelius.titan.diskstorage.configuration.{ ConfigOption, ConfigElement }
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.diskstorage.StandardStoreManager
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration
import org.cgnal.graphe.tinkerpop.config.TitanCGnalConfig

/**
 * Provides values to easily access Titan configuration settings, along with default values.
 */
package object hadoop {

  // key   = titanmr.ioformat.cf-name
  // value = edgestore
  val titanEdgeStoreNameKey   = ConfigElement.getPath(TitanHadoopConfiguration.COLUMN_FAMILY_NAME)
  val titanEdgeStoreNameValue = TitanHadoopConfiguration.COLUMN_FAMILY_NAME.getDefaultValue

  // key   = titanmr.ioformat.filter-partitioned-vertices
  // value = true
  val titanBulkPartitioningKey   = ConfigElement.getPath(TitanHadoopConfiguration.FILTER_PARTITIONED_VERTICES)
  val titanBulkPartitioningValue = true

  // key   = titanmr.ioformat.vertex-query
  // value = v.query()
  val titanVertexQueryKey   = ConfigElement.getPath(TitanCGnalConfig.VERTEX_QUERY)
  val titanVertexQueryValue = TitanCGnalConfig.VERTEX_QUERY.getDefaultValue

  // key   = storage.hbase.short-cf-names
  // value = true
  val titanShortenNameKey   = ConfigElement.getPath(HBaseStoreManager.SHORT_CF_NAMES)
  val titanShortenNameValue = HBaseStoreManager.SHORT_CF_NAMES.getDefaultValue

  // key   = storage.backend
  // value = hbase
  val titanBackendNameKey   = ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_BACKEND)
  val titanBackendNameValue = StandardStoreManager.HBASE.getShorthands.get(0)

  // key   = storage.hbase.table
  // value = titan
  val titanHBaseTableKey   = ConfigElement.getPath(HBaseStoreManager.HBASE_TABLE)
  val titanHBaseTableValue = HBaseStoreManager.HBASE_TABLE.getDefaultValue

  // key   = storage.hostname
  // value = 127.0.0.1
  val titanZookeeperQuorumKey   = ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_HOSTS)
  val titanZookeeperQuorumValue = GraphDatabaseConfiguration.STORAGE_HOSTS.getDefaultValue

  // key   = storage.port
  // value = 2181
  val titanZookeeperClientPortKey   = ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT)
  val titanZookeeperClientPortValue = 2181

  val titanZookeeperTimeoutKey   = "application.zookeeper.timeout"
  val titanZookeeperTimeoutValue = 10000

  val hbaseMapredInputTableKey    = "hbase.mapreduce.inputtable"
  val hbaseMapredScanKey          = "hbase.mapreduce.scan"
  val hbaseZookeeperQuorumKey     = "hbase.zookeeper.quorum"
  val hbaseZookeeperClientPortKey = "hbase.zookeeper.property.clientPort"
  val hbaseZookeeperTimeout       = "ha.zookeeper.session-timeout.ms"

}
