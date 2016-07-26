package org.cgnal.graphe.tinkerpop.graph

import scala.util.{ Try, Success, Failure }

import com.thinkaurelius.titan.core.TitanFactory
import com.thinkaurelius.titan.core.schema.TitanManagement

import org.cgnal.graphe.tinkerpop.config.ResourceConfig

object TitanGraphProvider extends TinkerGraphProvider with ResourceConfig with Serializable {

  @transient protected val graph = TitanFactory.open(config)

  def withGraphManagement[U](f: TitanManagement => U) = {
    val management = graph.openManagement()
    Try { f(management) } match {
      case Success(u) => management.commit(); u
      case Failure(e) => management.rollback(); throw e
    }
  }

}
