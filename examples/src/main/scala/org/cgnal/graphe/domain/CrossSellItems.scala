package org.cgnal.graphe.domain

import scala.util.matching.Regex
import scala.util.Random

case class CrossSellItems(productId: Int, crossSellProductId: Int, id: String)

object CrossSellItems {

  private def withRandomSize[A](maxSize: Int)(f: Int => A) = f { Random.nextInt(maxSize - 1) + 1 }

  private def _crossSellId(stream: Stream[Char], id: String = "", minLength: Int = 15, maxPartLength:Int = 5, parts: Int = 5): String = {
    if (id.length >= minLength) id
    else if (id.isEmpty) withRandomSize(maxPartLength) { n => _crossSellId(stream.drop(n), stream.take(n).mkString) }
    else                 withRandomSize(maxPartLength) { n => _crossSellId(stream.drop(n), s"$id-${stream.take(n).mkString}") }
  }

  def crossSellId = _crossSellId(Random.alphanumeric)

  def apply(productId: String, crossSellProductId: String): CrossSellItems = apply(productId.toInt, crossSellProductId.toInt, crossSellId)

  def fromString(s: String, extractorRegex: Regex = "(\\d+)\\s+(\\d+)".r): Option[CrossSellItems] = s match {
    case extractorRegex(productId, crossSellProductId) => Some { apply(productId, crossSellProductId) }
    case _                                             => None
  }

}
