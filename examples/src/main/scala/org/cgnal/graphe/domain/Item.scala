package org.cgnal.graphe.domain

case class Item(productId: Int)

object Item {

  def apply(productId: String): Item = apply { productId.toInt }

}
