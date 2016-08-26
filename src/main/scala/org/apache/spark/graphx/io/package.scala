package org.apache.spark.graphx

import org.apache.spark.graphx.impl.EdgePartition

package object io {

  /**
   * Alias for `(PartitionID, EdgePartition[A, B])`
   * @tparam A the vertex type
   * @tparam B the edge type
   */
  type PartitionEdge[A, B] = (PartitionID, EdgePartition[A, B])

  /**
   * Alias for `(VertexId, A)`
   * @tparam A the vertex type
   */
  type Vertex[A]           = (VertexId, A)

  /**
   * Sub-directory name where to load/save the java-serialized data.
   */
  def javaSuffix = "java"

  /**
   * Sub-directory name where to load/save the kryo-serialized data.
   */
  def kryoSuffix = "kryo"

  /**
   * Sub-directory name where to load/save the json-serialized data.
   */
  def jsonSuffix = "json"

  /**
   * Sub-directory name where to load/save vertex data.
   */
  def vertexLocation  = "vertices"

  /**
   * Sub-directory name where to load/save the edge data.
   */
  def edgeLocation    = "edges"

  /**
   * Sub-directory name where to load/save the triplet data.
   */
  def tripletLocation = "triplets"

  private def isVertexId(s: String): Boolean =
    if (s.isEmpty) false
    else if (s startsWith "-") s.tail.forall { _.isDigit }
    else s.forall { _.isDigit }

  /**
   * Attempts to transform a value into of type `Any` into `Long`, which can be used by Spark as a vertex id. Note that
   * the type-check is limited in scope to `Long`, `Int` and `String`.
   */
  def vertexId(any: Any): Long = any match {
    case l: Long => l
    case i: Int  => i
    case s: String if isVertexId(s) => s.toLong
    case other                      => throw new IllegalArgumentException(s"Vertex Id must be of of type [Long | Int] or a parsable numeric string, found [$other] of type [${other.getClass}]")
  }

}
