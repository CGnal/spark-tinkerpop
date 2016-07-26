package org.cgnal.graphe.tinkerpop

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration

package object hadoop {

  val titanEdgeStoreNameKey   = ConfigElement.getPath(TitanHadoopConfiguration.COLUMN_FAMILY_NAME)
  val titanEdgeStoreNameValue = TitanHadoopConfiguration.COLUMN_FAMILY_NAME.getDefaultValue

  val titanShortenNameKey   = ConfigElement.getPath(HBaseStoreManager.SHORT_CF_NAMES)
  val titanShortenNameValue = HBaseStoreManager.SHORT_CF_NAMES.getDefaultValue

  val titanHBaseTableKey   = ConfigElement.getPath(HBaseStoreManager.HBASE_TABLE)
  val titanHBaseTableValue = HBaseStoreManager.HBASE_TABLE.getDefaultValue

  val titanZookeeperQuorumKey   = ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_HOSTS)
  val titanZookeeperQuorumValue = GraphDatabaseConfiguration.STORAGE_HOSTS.getDefaultValue

  val titanZookeeperClientPortKey   = ConfigElement.getPath(GraphDatabaseConfiguration.STORAGE_PORT)
  val titanZookeeperClientPortValue = GraphDatabaseConfiguration.STORAGE_PORT.getDefaultValue

  val hbaseMapredInputTableKey    = "hbase.mapreduce.inputtable"
  val hbaseMapredScanKey          = "hbase.mapreduce.scan"
  val hbaseZookeeperQuorumKey     = "hbase.zookeeper.quorum"
  val hbaseZookeeperClientPortKey = "hbase.zookeeper.property.clientPort"

}
