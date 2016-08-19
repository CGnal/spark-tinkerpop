package org.cgnal.graphe.tinkerpop

import scala.collection.convert.decorateAsScala._

import org.apache.commons.configuration.{ Configuration => TinkerConfig }

import org.apache.hadoop.conf.{ Configuration => HadoopConfig }

import com.thinkaurelius.titan.diskstorage.configuration.ConfigElement
import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.diskstorage.StandardStoreManager
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration

package object hadoop {

  // key   = titanmr.ioformat.cf-name
  // value = edgestore
  val titanEdgeStoreNameKey   = ConfigElement.getPath(TitanHadoopConfiguration.COLUMN_FAMILY_NAME)
  val titanEdgeStoreNameValue = TitanHadoopConfiguration.COLUMN_FAMILY_NAME.getDefaultValue

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

  val hbaseMapredInputTableKey    = "hbase.mapreduce.inputtable"
  val hbaseMapredScanKey          = "hbase.mapreduce.scan"
  val hbaseZookeeperQuorumKey     = "hbase.zookeeper.quorum"
  val hbaseZookeeperClientPortKey = "hbase.zookeeper.property.clientPort"


  implicit class EnrichedHadoopConfig(hadoopConfig: HadoopConfig) {

    def copy = hadoopConfig.asScala.foldLeft(new HadoopConfig) { (accConf, entry) =>
      accConf.set(entry.getKey, entry.getValue)
      accConf
    }

    def mergeWith(tinkerConfig: TinkerConfig) = tinkerConfig.getKeys.asScala.foldLeft(copy) { (hadoop, next) =>
      hadoop.set(next, tinkerConfig.getProperty(next).toString)
      hadoop
    }

  }

}
