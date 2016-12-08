package org.cgnal.graphe.tinkerpop.titan.hadoop

import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryRecordReader

import org.apache.hadoop.conf.{ Configuration, Configurable }
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapred.TableMapReduceUtil
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext }
import org.apache.hadoop.security.UserGroupInformation

import org.cgnal.graphe.tinkerpop.NativeGraphInputFormat
import org.cgnal.graphe.EnrichedHadoopConfig

/**
 * `NativeInputFormat` implementation for Titan's hbase storage backend.
 */
class TitanHbaseInputFormat extends NativeGraphInputFormat with Configurable {

  private var config: JobConf = _

  private lazy val tableInputFormat = new TableInputFormat

  def getConf = config
  def setConf(conf: Configuration) = synchronized {
    config = HBaseConfiguration.create(conf).asJobConf

    conf.set       (hbaseMapredInputTableKey   , titanHbaseTable)
    conf.setStrings(hbaseZookeeperQuorumKey    , titanZookeeperQuorum: _*)
    conf.setInt    (hbaseZookeeperClientPortKey, titanZookeeperClientPort)
    conf.set       (hbaseMapredScanKey         , titanHbaseScanString)
    conf.setInt    (hbaseZookeeperTimeout      , titanZookeeperTimeout)

    TableMapReduceUtil.initCredentials(config)
    config.getCredentials.addAll { UserGroupInformation.getCurrentUser.getCredentials }
    tableInputFormat.setConf(config)
  }

  private def titanFullEdgeStoreFamily = getConf.get       (titanEdgeStoreNameKey      , titanEdgeStoreNameValue)
  private def titanShortenNames        = getConf.getBoolean(titanShortenNameKey        , titanShortenNameValue)
  private def titanHbaseTable          = getConf.get       (titanHBaseTableKey         , titanHBaseTableValue)
  private def titanZookeeperClientPort = getConf.getInt    (titanZookeeperClientPortKey, titanZookeeperClientPortValue)
  private def titanZookeeperQuorum     = getConf.getStrings(titanZookeeperQuorumKey    , titanZookeeperQuorumValue: _*)
  private def titanZookeeperTimeout    = getConf.getInt    (titanZookeeperTimeoutKey   , titanZookeeperTimeoutValue)

  private def titanEdgeStorageFamily   = if (titanShortenNames) HBaseStoreManager.shortenCfName { titanFullEdgeStoreFamily } else titanFullEdgeStoreFamily

  private def titanHbaseScan(scanner: Scan = new Scan) = scanner.addFamily { Bytes.toBytes(titanEdgeStorageFamily) }
  private def titanHbaseScanString = Base64.encodeBytes { ProtobufUtil.toScan(titanHbaseScan()).toByteArray }

  private def withUpdatedConf[A](newConf: Configuration)(f: Configuration => A) = {
    setConf(newConf)
    f { getConf }

  }

  private def createHbaseRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = withUpdatedConf(taskContext.getConfiguration) { _ =>
    new HBaseBinaryRecordReader(
      tableInputFormat.createRecordReader(inputSplit, taskContext),
      Bytes.toBytes(titanEdgeStorageFamily)
    )
  }

  def getSplits(context: JobContext) = withUpdatedConf(context.getConfiguration) { _ =>
    tableInputFormat.getSplits(context)
  }

  def createRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = new TitanHbaseRecordReader(createHbaseRecordReader(inputSplit, taskContext), getConf)

}
