package org.cgnal.graphe.tinkerpop.graph.query

import javax.script.Bindings

import scala.reflect.ClassTag

/**
 * Container for the result of evaluating a script against a set of `Bindings` using a `ScriptEngine` implementation;
 * refer to `CompiledQuery::eval`
 * @param result the generic result of evaluating a `CompiledScript` with a set of `Bindings`
 * @param bindings the `Bindings` that were used to evaluate the script
 * @param _queryType the type of query that generated this result
 * @tparam A the type of the `result`
 */
final case class ScriptResult[A](result: A, bindings: Bindings, private val _queryType: QueryType) {

  private def unrefinableResult[B](B: Class[_]): PartialFunction[(A, QueryType), B] = {
    case _ => throw new RuntimeException(s"Cannot refine class of type [${result.getClass.getName}] with query type [${_queryType}] into [${B.getName}]")
  }

  /**
   * Applies a `PartialFunction` to the `result` and `QueryType` to refine it to a required type; this is similar to a
   * Functor operation but not identical, because the refinement operation `pf` exists under restrictions and
   * limitations:
   *   1. only expected to '''partially''' map elements of type `A`
   *   1. assumed to require the `QueryType` as input to the partital function
   *   1. type-bound by `A`
   * @param pf the partial function used to apply mapping to a subset of `A`; when the function is not defined for a
   *           specific `A`, an `RuntimeException` is thrown by default
   * @tparam B the type of the result returned by the application of `pf` on `result`, which must be a sub-type of `A`
   * @return a new `ScriptResult` instance, whose `result` value is the result of applying successfully `pf` on the
   *         current `result`
   */
  def refine[B <: A](pf: PartialFunction[(A, QueryType), B])(implicit B: ClassTag[B]): ScriptResult[B] = this.copy[B](
    result = pf orElse unrefinableResult[B](B.runtimeClass) apply (this.result, _queryType)
  )

}
