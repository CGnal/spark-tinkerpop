package org.cgnal.graphe.arrows

import org.cgnal.graphe.domain.{ CrossSellItems, Item }
import org.cgnal.graphe.tinkerpop.Arrows
import org.cgnal.graphe.tinkerpop.Arrows.{ TinkerVertexPropMap, TinkerPropMap }

sealed class ItemRawPropSetArrow extends Arrows.TinkerRawPropSetArrowF[Item] with Arrows.TinkerRawPropSetArrowR[Item] {

  def apF(item: Item) = Map(
    "productId" -> Int.box { item.productId }
  )

  def apR(map: Map[String, AnyRef]) = Item(
    map.getOrElse("productId", -1).asInstanceOf[Int]
  )

}

sealed class ItemVertexPropSetArrow(implicit arrowF: Arrows.TinkerRawPropSetArrowF[Item], arrowR: Arrows.TinkerRawPropSetArrowR[Item]) extends Arrows.TinkerVertexPropSetArrowF[Item, AnyRef] with Arrows.TinkerVertexPropSetArrowR[Item, AnyRef] {

  def apF(a: Item): TinkerVertexPropMap[AnyRef] = Arrows.tinkerVertexPropSetArrowF(arrowF).apF(a)

  def apR(b: TinkerVertexPropMap[AnyRef]): Item = Arrows.tinkerVertexPropSetArrowR(arrowR).apR(b)

}

sealed class CrossSellItemsRawPropSetArrow extends Arrows.TinkerRawPropSetArrowF[CrossSellItems] with Arrows.TinkerRawPropSetArrowR[CrossSellItems] {

  def apF(crossSell: CrossSellItems) = Map(
    "productId"    -> Int.box { crossSell.productId },
    "crossSellId"  -> Int.box { crossSell.crossSellProductId },
    "uniqueId"     -> crossSell.id
  )

  def apR(map: Map[String, AnyRef]) = CrossSellItems(
    map("productId").asInstanceOf[Int],
    map("crossSellId").asInstanceOf[Int],
    map("uniqueId").asInstanceOf[String]
  )

}

sealed class CrossSellItemPropSetArrow(implicit arrowF: Arrows.TinkerRawPropSetArrowF[CrossSellItems], arrowR: Arrows.TinkerRawPropSetArrowR[CrossSellItems]) extends Arrows.TinkerPropSetArrowF[CrossSellItems, AnyRef] with Arrows.TinkerPropSetArrowR[CrossSellItems, AnyRef] {

  def apF(a: CrossSellItems): TinkerPropMap[AnyRef] = Arrows.tinkerPropSetArrowF(arrowF).apF(a)

  def apR(b: TinkerPropMap[AnyRef]): CrossSellItems = Arrows.tinkerPropSetArrowR(arrowR).apR(b)

}

object DomainArrows extends Serializable {

  implicit val rawItemPropSetArrow: Arrows.TinkerRawPropSetArrowF[Item] with  Arrows.TinkerRawPropSetArrowR[Item] = new ItemRawPropSetArrow

  implicit val itemTinkerPropSetArrow: Arrows.TinkerVertexPropSetArrowF[Item, AnyRef] with Arrows.TinkerVertexPropSetArrowR[Item, AnyRef] = new ItemVertexPropSetArrow

  implicit val rawCrossSellItemsPropSetArrow: Arrows.TinkerRawPropSetArrowF[CrossSellItems] with  Arrows.TinkerRawPropSetArrowR[CrossSellItems] = new CrossSellItemsRawPropSetArrow

  implicit val crossSellItemsTinkerPropSetArrow: Arrows.TinkerPropSetArrowF[CrossSellItems, AnyRef] with Arrows.TinkerPropSetArrowR[CrossSellItems, AnyRef] = new CrossSellItemPropSetArrow

  implicit val itemTinkerVertexArrow: Arrows.TinkerVertexArrowR[Item] = Arrows.tinkerVertexArrowR(itemTinkerPropSetArrow)

  implicit val crossSellItemsTinkerVertexArrow: Arrows.TinkerEdgeArrowR[CrossSellItems] = Arrows.tinkerEdgeArrowR(crossSellItemsTinkerPropSetArrow)

}
