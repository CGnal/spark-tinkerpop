package org.cgnal.graphe.tinkerpop

import org.apache.spark.graphx.Edge

case class TinkerpopPoint[A](in: Edge[A], vertexId: Long, out: Edge[A]) {

  def withIn(otherIn: Edge[A]) = this.copy(in = otherIn)
  def ->:(otherIn: Edge[A]) = withIn(otherIn)

  def withOut(otherOut: Edge[A]) = this.copy(out = otherOut)
  def :->(otherOut: Edge[A]) = withOut(otherOut)

}

object TinkerpopPoint {

  def fromIn[A](in: Edge[A]) = apply(in, in.dstId, null)
  def fromOut[A](out: Edge[A]) = apply(out, out.srcId, null)

  def solitary[A](vertexId: Long) = apply[A](null, vertexId, null)

}
