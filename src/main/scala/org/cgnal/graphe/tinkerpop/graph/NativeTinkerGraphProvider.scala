package org.cgnal.graphe.tinkerpop.graph

import org.apache.spark.rdd.RDD

import org.cgnal.graphe.tinkerpop.{Arrows, TinkerpopEdges}

trait NativeTinkerGraphProvider extends TinkerGraphProvider { this: Serializable =>

  def saveNative[A, B](rdd: RDD[TinkerpopEdges[A, B]], useTinkerpop: Boolean = false)(implicit arrowV: Arrows.TinkerRawPropSetArrowF[A], arrowE: Arrows.TinkerRawPropSetArrowF[B])

}
