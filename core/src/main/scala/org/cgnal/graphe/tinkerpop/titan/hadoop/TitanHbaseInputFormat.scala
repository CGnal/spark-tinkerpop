package org.cgnal.graphe.tinkerpop.titan.hadoop

import scala.collection.convert.decorateAsScala._

import org.apache.hadoop.conf.{ Configuration, Configurable }
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{ Base64, Bytes }
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext }

import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryRecordReader

import org.cgnal.graphe.tinkerpop.NativeGraphInputFormat

/**
 * `NativeInputFormat` implementation for Titan's hbase storage backend.
 */
class TitanHbaseInputFormat extends NativeGraphInputFormat with Configurable {

  private var config = new Configuration

  private lazy val tableInputFormat = new TableInputFormat

  def getConf = config
  def setConf(conf: Configuration) = {
    config = conf
    conf.set       (hbaseMapredInputTableKey   , titanHbaseTable)
    conf.setStrings(hbaseZookeeperQuorumKey    , titanZookeeperQuorum: _*)
    conf.setInt    (hbaseZookeeperClientPortKey, titanZookeeperClientPort)
    conf.set       (hbaseMapredScanKey         , titanHbaseScanString)
    tableInputFormat.setConf(conf)
  }

  private def titanFullEdgeStoreFamily = getConf.get       (titanEdgeStoreNameKey      , titanEdgeStoreNameValue)
  private def titanShortenNames        = getConf.getBoolean(titanShortenNameKey        , titanShortenNameValue)
  private def titanHbaseTable          = getConf.get       (titanHBaseTableKey         , titanHBaseTableValue)
  private def titanZookeeperClientPort = getConf.getInt    (titanZookeeperClientPortKey, titanZookeeperClientPortValue)
  private def titanZookeeperQuorum     = getConf.getStrings(titanZookeeperQuorumKey    , titanZookeeperQuorumValue: _*)

  private def titanEdgeStorageFamily   = if (titanShortenNames) HBaseStoreManager.shortenCfName { titanFullEdgeStoreFamily } else titanFullEdgeStoreFamily

  private def titanHbaseScan(scanner: Scan = new Scan) = scanner.addFamily { Bytes.toBytes(titanEdgeStorageFamily) }
  private def titanHbaseScanString = Base64.encodeBytes { ProtobufUtil.toScan(titanHbaseScan()).toByteArray }

  private def withUpdatedConf[A](newConf: Configuration)(f: Configuration => A) = {
    setConf(newConf)
    f { getConf }
  }

  private def createHbaseRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = new HBaseBinaryRecordReader(
    tableInputFormat.createRecordReader(inputSplit, taskContext),
    Bytes.toBytes(titanEdgeStorageFamily))

  def getSplits(context: JobContext) = tableInputFormat.getSplits(context)

  def createRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = withUpdatedConf(taskContext.getConfiguration) {
    new TitanHbaseRecordReader(createHbaseRecordReader(inputSplit, taskContext), _)
  }

}
