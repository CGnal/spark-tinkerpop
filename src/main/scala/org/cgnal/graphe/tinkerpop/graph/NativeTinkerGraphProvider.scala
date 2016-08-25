package org.cgnal.graphe.tinkerpop.graph

import org.apache.hadoop.io.NullWritable

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.tinkerpop.gremlin.hadoop.structure.io.VertexWritable
import org.apache.tinkerpop.gremlin.structure.{ Vertex => TinkerVertex }

import org.cgnal.graphe.tinkerpop.{ Arrows, TinkerpopEdges, NativeGraphInputFormat }
import org.cgnal.graphe.EnrichedHadoopConfig

trait NativeTinkerGraphProvider extends TinkerGraphProvider { this: Serializable =>

  protected def nativeInputFormat: Class[_ <: NativeGraphInputFormat]

  def loadNative(sparkContext: SparkContext): RDD[TinkerVertex] = sparkContext.newAPIHadoopRDD[NullWritable, VertexWritable, NativeGraphInputFormat](
    sparkContext.hadoopConfiguration.mergeWith { tinkerConfig },
    nativeInputFormat.asInstanceOf[Class[NativeGraphInputFormat]],
    classOf[NullWritable],
    classOf[VertexWritable]
  ).map { _._2.get() }

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B])

}
