package org.cgnal.graphe.tinkerpop.graph.query

import javax.script.{ Bindings, SimpleBindings }

import scala.collection.convert.decorateAsScala._
import scala.reflect.ClassTag

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalStrategies
import org.apache.tinkerpop.gremlin.process.traversal.{ TraversalStrategy, TraversalStrategies }
import org.apache.tinkerpop.gremlin.structure.{ Edge, Vertex, Graph }
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraph

import org.cgnal.graphe.tinkerpop.{ EnrichedGraphTraversal, EnrichedTinkerVertex }

/**
 * Extends `QueryParsing` to implement a way to create bindings from Tinkerpop `Vertex` instances, adding
 * `evalTraversal` functions that process specific Tinkerpop `GraphTraversal` instances returned by the gremlin script
 * evaluation using a StarGraph.
 */
trait TinkerpopQueryParsing extends QueryParsing[Vertex] with GroovyQueryParsing[Vertex] {

  private type VertexTraversal = GraphTraversal[Vertex, Vertex]
  private type EdgeTraversal   = GraphTraversal[Edge, Edge]

  private def addBinding(element: Vertex, queryType: QueryType, bindings: Bindings = new SimpleBindings()) = queryType match {
    case VertexQueryType => bindings.put(VertexQueryType.binding, element.graph.traversal.V()); bindings
    case EdgeQueryType   => bindings.put(EdgeQueryType.binding  , element.graph.traversal.E()); bindings
  }

  final protected def createBindings(element: Vertex, script: String) = addBinding(element, QueryTypes.fromScript(script))

  private def processVertexTraversal(element: Vertex, traversal: GraphTraversal[Vertex, Vertex]) = traversal.unlessEmpty { _ => element }

  private def processEdgeTraversal(element: Vertex, traversal: GraphTraversal[Edge, Edge]) = traversal.unlessEmpty {
    _.foldLeft[Vertex](StarGraph.of(element).getStarVertex) { _ addEdge _ }
  }

  private def processTraversal(element: Vertex): PartialFunction[(AnyRef, QueryType), Option[Vertex]] = {
    case (traversal, VertexQueryType) => processVertexTraversal(element, traversal.asInstanceOf[VertexTraversal])
    case (traversal, EdgeQueryType)   => processEdgeTraversal(element, traversal.asInstanceOf[EdgeTraversal])
  }

  /**
   * Applies `eval` and refines the resulting `AnyRef` into `Option[Vertex]` assuming the resulting `AnyRef` can be
   * refined to a `GraphTraversal` instance. If the resulting `Traversal` is empty, the `ScriptResult` will contain
   * `None`.
   * @param element the Tinkerpop `Vertex` on which the script should be applied
   * @param script the script that should be applied on the `Vertex` `element`
   * @return a `ScriptResult` containing `Some` with the result of the traversal or `None` if the traversal returned
   *         nothing
   */
  final protected def evalTraversal(element: Vertex, script: String): ScriptResult[Option[Vertex]] = evalTraversal(element, compileScript(script))

  /**
   * Applies `eval` and refines the resulting `AnyRef` into `Option[Vertex]` assuming the resulting `AnyRef` can be
   * refined to a `GraphTraversal` instance. If the resulting `Traversal` is empty, the `ScriptResult` will contain
   * `None`.
   * @param element the Tinkerpop `Vertex` on which the script should be applied
   * @param script the `CompiledQuery` that should be applied on the `Vertex` `element`
   * @return a `ScriptResult` containing `Some` with the result of the traversal or `None` if the traversal returned
   *         nothing
   */
  final protected def evalTraversal(element: Vertex, script: CompiledQuery): ScriptResult[Option[Vertex]] = eval(element, script).refine { processTraversal(element) }

}

trait TraversalOptimizations { this: TinkerpopQueryParsing =>

  protected def newStrategies: Seq[TraversalStrategy[_]] = Seq.empty

  final protected def basicStrategies = TraversalStrategies.GlobalCache.getStrategies(classOf[Graph]).toList.asScala

  final protected def allStrategies = newStrategies ++ basicStrategies

  final protected  def allTraversalStrategies = new DefaultTraversalStrategies().addStrategies(allStrategies: _*)

  final protected def useOptimizations[B <: Graph](implicit B: ClassTag[B]) = TraversalStrategies.GlobalCache.registerStrategies(B.runtimeClass, allTraversalStrategies)

}
