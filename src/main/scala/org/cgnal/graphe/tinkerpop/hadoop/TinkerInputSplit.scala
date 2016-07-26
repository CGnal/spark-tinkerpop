package org.cgnal.graphe.tinkerpop.hadoop

case class TinkerInputSplit(start: Long, end: Long) {

  def length = end - start + 1

}

object TinkerInputSplit {

  def startFrom(start: Long) = apply(start, Long.MaxValue)
  def endAt(end: Long) = apply(Long.MinValue, end)

}
