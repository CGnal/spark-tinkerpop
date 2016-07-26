package org.cgnal.graphe.tinkerpop.hadoop

import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.{ Base64, Bytes }

import org.apache.hadoop.conf.{ Configuration, Configurable }
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, JobContext, InputFormat }
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable

import com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager
import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryRecordReader

class TitanHbaseInputFormat extends InputFormat[NullWritable, VertexWritable] with Configurable {

  private var config = new Configuration

  private lazy val tableInputFormat = new TableInputFormat

  def getConf = config
  def setConf(conf: Configuration) = {
    conf.set       (hbaseMapredInputTableKey   , titanHbaseTable)
    conf.setStrings(hbaseZookeeperQuorumKey    , titanZookeeperQuorum: _*)
    conf.setInt    (hbaseZookeeperClientPortKey, titanZookeeperClientPort)
    conf.set       (hbaseMapredScanKey         , titanHbaseScanString)
    config = conf
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

  def getSplits(context: JobContext) = tableInputFormat.getSplits(context)

  def createHbaseRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = new HBaseBinaryRecordReader(
    tableInputFormat.createRecordReader(inputSplit, taskContext),
    Bytes.toBytes(titanEdgeStorageFamily)
  )

  def createRecordReader(inputSplit: InputSplit, taskContext: TaskAttemptContext) = new TitanHbaseRecordReader(
    createHbaseRecordReader(inputSplit, taskContext),
    getConf
  )



}
