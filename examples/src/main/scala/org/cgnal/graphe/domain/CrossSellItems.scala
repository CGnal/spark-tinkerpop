package org.cgnal.graphe.domain

import scala.util.matching.Regex

case class CrossSellItems(productId: Int, crossSellProductId: Int, id: Int)

object CrossSellItems {

  def apply(productId: String, crossSellProductId: String): CrossSellItems = apply(productId.toInt, crossSellProductId.toInt, util.Random.nextInt(5000))

  def fromString(s: String, extractorRegex: Regex = "(\\d+)\\s+(\\d+)".r): Option[CrossSellItems] = s match {
    case extractorRegex(productId, crossSellProductId) => Some { apply(productId, crossSellProductId) }
    case _                                             => None
  }

}
