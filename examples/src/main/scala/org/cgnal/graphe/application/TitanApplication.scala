package org.cgnal.graphe.application

import java.lang.{ Integer => JavaInt }

import scala.collection.convert.decorateAsScala._
import scala.util.{ Try, Success, Failure }

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import org.apache.tinkerpop.gremlin.process.traversal.P

import com.thinkaurelius.titan.core.Multiplicity.MULTI
import com.thinkaurelius.titan.core.Cardinality.{ SINGLE, SET }

import org.cgnal.graphe.application.config.{ ExampleApplicationConfigReader, ExampleApplicationConfig }
import org.cgnal.graphe.arrows.DomainArrows._
import org.cgnal.graphe.domain.{ Item, CrossSellItems }
import org.cgnal.graphe.tinkerpop.{ EnrichedSparkTinkerGraph, EnrichedTinkerSparkContext }
import org.cgnal.graphe.tinkerpop.graph.NativeTinkerGraphProvider
import org.cgnal.graphe.tinkerpop.titan.{ TitanGraphProvider, EnrichedTitanManagement }
import org.cgnal.graphe.application.Providers.tinkerGraphProvider

sealed class TitanApplication(protected val sparkContext: SparkContext,
                              val config: ExampleApplicationConfig) extends Application with SparkContextInstance {

  private def loadData() = Try {
    sparkContext.textFile(config.inputFileLocation).collect {
      case line if !line.startsWith("#") => CrossSellItems.fromString(line)
    }.collect { case Some(item) => item }.persist("Input")
  }

  private def loadVertices(rdd: RDD[CrossSellItems]) = Try {
    rdd.flatMap {
      case CrossSellItems(productId, crossSellProductId, _) if crossSellProductId != -1 => Seq(productId.toLong -> Item(productId), crossSellProductId.toLong -> Item(crossSellProductId))
      case CrossSellItems(productId, _, _)                                              => Seq(productId.toLong -> Item(productId))
    }.distinct().persist("Vertices")
  }

  private def loadEdges(rdd: RDD[CrossSellItems]) = Try {
    rdd.collect {
      case crossSell if crossSell.crossSellProductId != -1 => Edge(crossSell.productId, crossSell.crossSellProductId, crossSell)
    }.persist("Edges")
  }

  private def createTitanSchema() = Try {
    TitanGraphProvider.withGraphManagement { m =>
      m.createVertexLabel("Item") { identity }

      m.createEdgeLabel("CrossSellItems") { _.multiplicity(MULTI) }

      m.createPropertyKey[JavaInt]("productId")   { _.cardinality(SINGLE) }
      m.createPropertyKey[JavaInt]("crossSellId") { _.cardinality(SINGLE) }
      m.createPropertyKey[String] ("uniqueId")    { _.cardinality(SET) }
    }
  }

  private def graphX(vertices: RDD[(VertexId, Item)], edges: RDD[Edge[CrossSellItems]]) = Try { Graph(vertices, edges) }

  private def loadGraph()(implicit provider: NativeTinkerGraphProvider) = Try {
    sparkContext.loadNativeContainer[Item, CrossSellItems]
  }

  private def saveGraph(graph: Graph[Item, CrossSellItems])(implicit provider: NativeTinkerGraphProvider) = Try {
    graph.saveNativeGraph()
  }

  private def queryVertices(implicit provider: NativeTinkerGraphProvider) = Try {
    val vertices = provider.withGraph { g => g.traversal().V().toList.asScala }
    vertices.foreach { vertex =>
      log.trace { s"v: { id = [${vertex.id()}] label = [${vertex.label()}] props = [${vertex.properties().asScala.mkString(", ")}] }" }
    }
    log.info { s"Found [${vertices.size}] vertices" }
  }

  private def queryEdges(implicit provider: NativeTinkerGraphProvider) = Try {
    val edges = provider.withGraph { g => g.traversal().E().toList.asScala }
    edges.foreach { edge =>
      log.trace { s"e: { id = [${edge.id()}] label = [${edge.label()}] props = [${edge.properties().asScala.mkString(", ")}] }" }
    }
    log.info { s"Found [${edges.size}] edges" }
  }

  private def save() = for {
    data     <- timed("Loading data")        { loadData()          }
    vertices <- timed("Generating Vertices") { loadVertices(data)  }
    edges    <- timed("Generating Edges")    { loadEdges(data)     }
    _        <- timed("Showing Vertices")    { show(vertices)      }
    _        <- timed("Showing Edges")       { show(edges)         }
    graph    <- graphX(vertices, edges)
    _        <- timed("Generating Schema")   { createTitanSchema() }
    _        <- timed("Saving Graph")        { saveGraph(graph)    }
    _        <- timed("Vertex Query")        { queryVertices       }
    _        <- timed("Edge Query")          { queryEdges          }
  } yield ()

  private def load() = for {
    graphContainer <- timed("Loading graph")       { loadGraph()                      }
    _              <- timed("Recovering Vertices") { show(graphContainer.graph.vertices.values)      }
    _              <- timed("Recovering Edges")    { show(graphContainer.graph.edges.map { _.attr }) }
    _              <- timed("Unpersisting input")  { Try(graphContainer.unpersistInput()) }
  } yield ()

  private def tearDown() = if (config.tearDown) {
    TitanGraphProvider.clearGraph() match {
      case Success(_) => log.info("Instance successfully torn down")
      case Failure(e) => log.error("Instance tear-down failed", e)
    }
  }

  private def runBody() = for {
    _ <- save()
    _ <- load()
  } yield ()

  def run() = runBody().transform(
    _     => Try { tearDown() },
    error => { tearDown(); Failure(error) }
  )

}

object TitanApplication extends SparkContextProvider {

  def create(args: Seq[String]) = ExampleApplicationConfigReader.withConfig(args) { conf =>
    withSparkContext("Titan", conf) { sparkContext => new TitanApplication(sparkContext, conf) }
  }

}