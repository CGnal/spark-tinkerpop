package org.cgnal.graphe.tinkerpop.graph.query

/**
 * Indicates the type of query being executed by the engine, together with the binding this type of query requires.
 */
sealed trait QueryType {

  /**
   * The name of the binding that must be present for a query of this type to evaluate without binding errors.
   */
  def binding: String

}

/**
 * Indicates that the query will operate directly on vertex values.
 */
case object VertexQueryType extends QueryType {
  def binding = "v"
}

/**
 * Indicates that the query will operate on edge values.
 */
case object EdgeQueryType extends QueryType {
  def binding = "e"
}

/**
 * Construction helper object, used to create a `QueryType` instance from other more primitive types, such as `String`.
 */
object QueryTypes {

  private def trim(s: String) = if (s.length > 50) s"${s.take(47)}..." else s

  def fromScript(script: String) =
    if      (script startsWith "v.") VertexQueryType
    else if (script startsWith "e.") EdgeQueryType
    else    throw new IllegalArgumentException(s"Invalid script: [${trim(script)}]: must start with [v] for vertices and [e] for edges")

}
