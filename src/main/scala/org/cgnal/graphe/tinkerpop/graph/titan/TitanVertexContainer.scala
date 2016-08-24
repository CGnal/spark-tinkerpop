package org.cgnal.graphe.tinkerpop.graph.titan

import com.thinkaurelius.titan.core.TitanVertex

import org.cgnal.graphe.tinkerpop.Arrows

private[titan] case class TitanVertexContainer[A](a: A, vertex: TitanVertex) {

  def enrich(implicit arrow: Arrows.TinkerRawPropSetArrowF[A]) = arrow.apF(a).foreach { case (key, value) =>
    vertex.property(key, value, Seq.empty[AnyRef]: _*)
  }

}