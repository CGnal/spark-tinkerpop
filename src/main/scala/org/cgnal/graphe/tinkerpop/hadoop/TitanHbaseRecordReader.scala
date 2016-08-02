package org.cgnal.graphe.tinkerpop.hadoop

import scala.collection.convert.decorateAsScala._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, RecordReader }
import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryRecordReader
import com.thinkaurelius.titan.hadoop.formats.util.TitanVertexDeserializer
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup
import com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl

class TitanHbaseRecordReader(hbaseReader: HBaseBinaryRecordReader, config: Configuration) extends RecordReader[NullWritable, VertexWritable] {

  private var vertexWritable: VertexWritable = new VertexWritable()

  private lazy val titanHadoopSetup: TitanHadoopSetup = {
    config.iterator().asScala.foreach { entry =>
      println { s"{${entry.getKey} : [${entry.getValue}]}" }
    }
    new TitanHadoopSetupImpl(config)
  }
  private lazy val vertexReader = new TitanVertexDeserializer(titanHadoopSetup)

  private def setCurrentValue(vertex: TinkerVertex) = {
    vertexWritable = new VertexWritable(vertex)
    true
  }

  private def readKeyValue() = Option { vertexReader.readHadoopVertex(hbaseReader.getCurrentKey, hbaseReader.getCurrentValue) } match {
    case Some(tinkerVertex) => setCurrentValue(tinkerVertex)
    case None               => nextKeyValue()
  }

  def initialize(split: InputSplit, context: TaskAttemptContext) = hbaseReader.initialize(split, context)

  def getProgress = hbaseReader.getProgress

  def nextKeyValue(): Boolean = if (hbaseReader.nextKeyValue()) readKeyValue() else false

  def getCurrentValue = vertexWritable

  def getCurrentKey = NullWritable.get()

  def close() = {
    vertexReader.close()
    hbaseReader.close()
  }
}
