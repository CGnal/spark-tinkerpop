package org.apache.spark.graphx

import org.apache.spark.graphx.impl.EdgePartition

package object io {

  type PartitionEdge[A, B] = (PartitionID, EdgePartition[A, B])
  type Vertex[A]           = (VertexId, A)

  def javaSuffix = "java"
  def kryoSuffix = "kryo"
  def jsonSuffix = "json"

  def vertexLocation  = "vertices"
  def edgeLocation    = "edges"
  def tripletLocation = "triplets"

  private def isVertexId(s: String): Boolean =
    if (s.isEmpty) false
    else if (s startsWith "-") s.tail.forall { _.isDigit }
    else s.forall { _.isDigit }

  def vertexId(any: Any): Long = any match {
    case l: Long => l
    case i: Int  => i
    case s: String if isVertexId(s) => s.toLong
    case other                      => throw new IllegalArgumentException(s"Vertex Id must be of of type [Long | Int] or a parsable numeric string, found [$other] of type [${other.getClass}]")
  }

}
