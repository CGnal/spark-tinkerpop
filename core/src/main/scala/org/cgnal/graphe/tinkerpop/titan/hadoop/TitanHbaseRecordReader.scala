package org.cgnal.graphe.tinkerpop.titan.hadoop

import scala.collection.convert.decorateAsScala._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.{ TaskAttemptContext, InputSplit, RecordReader }

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

import com.thinkaurelius.titan.hadoop.formats.hbase.HBaseBinaryRecordReader
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup
import com.thinkaurelius.titan.hadoop.formats.util.input.current.TitanHadoopSetupImpl
import com.thinkaurelius.titan.graphdb.tinkerpop.optimize.AdjacentVertexFilterOptimizerStrategy

import org.cgnal.graphe.tinkerpop.graph.query.{ TinkerpopQueryParsing, TraversalOptimizations }

class TitanHbaseRecordReader(hbaseReader: HBaseBinaryRecordReader, config: Configuration)
  extends RecordReader[NullWritable, VertexWritable]
  with TinkerpopQueryParsing
  with TraversalOptimizations {

  override protected def newStrategies = Seq(
    AdjacentVertexFilterOptimizerStrategy.instance()
  )

  private var vertexWritable: VertexWritable = new VertexWritable()

  private lazy val groovyQueryString = config.get(titanVertexQueryKey)
  private lazy val tinkerpopQuery    = compileScript(groovyQueryString)

  private lazy val titanHadoopSetup: TitanHadoopSetup = new TitanHadoopSetupImpl(config)
  private lazy val vertexReader = new TitanHbaseVertexReader(titanHadoopSetup)

  private lazy val usesScript = groovyQueryString match {
    case null | "" | "none" => false
    case _                  => useOptimizations[TinkerGraph]; true
  }

  private def evalQuery(vertex: TinkerVertex) = if (usesScript) evalTraversal(vertex, tinkerpopQuery).result else Some(vertex)

  private def setCurrentValue(vertex: TinkerVertex) = {
    vertexWritable = new VertexWritable(vertex)
    true
  }

  private def readKeyValue() = Option { vertexReader.readVertex(hbaseReader.getCurrentKey, hbaseReader.getCurrentValue.asScala) }.flatMap { evalQuery } match {
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
