package org.cgnal.graphe.tinkerpop.titan

import com.thinkaurelius.titan.core.TitanVertex

import org.cgnal.graphe.tinkerpop.Arrows

/**
 * Simple container element that provides a suffix method for easily enriching a `TitanVertex` instance with all the
 * properties of `a` as transformed by the application of an implicitly available `Arrow`.
 * @param a the original vertex intance
 * @param vertex the `TitanVertex` instance that was created from `a`
 * @tparam A the vertex type
 */
private[titan] case class TitanVertexContainer[A](a: A, vertex: TitanVertex) {

  /**
   * Applies the `Arrow` onto `a` to produce the set of properties that are used to enrich the vertex.
   * @param arrow the transformation `Arrow` for `a` into a `Seq[AnyRef]`, which represents
   *              `Seq(key1, value1, key2, value2, ...)`
   */
  def enrich(implicit arrow: Arrows.TinkerRawPropSetArrowF[A]): Unit = arrow.apF(a).foreach { case (key, value) =>
    vertex.property(key, value, Seq.empty[AnyRef]: _*)
  }

}