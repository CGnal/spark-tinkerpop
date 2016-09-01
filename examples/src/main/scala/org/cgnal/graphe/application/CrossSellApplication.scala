package org.cgnal.graphe.application

import java.lang.{ Integer => JavaInt }

import scala.util.{ Try, Success, Failure }

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import com.thinkaurelius.titan.core.Multiplicity.MULTI
import com.thinkaurelius.titan.core.Cardinality.SINGLE

import org.cgnal.graphe.arrows.DomainArrows._
import org.cgnal.graphe.domain.{Item, CrossSellItems}
import org.cgnal.graphe.tinkerpop.{ EnrichedSparkTinkerGraph, EnrichedTinkerSparkContext }
import org.cgnal.graphe.tinkerpop.graph.NativeTinkerGraphProvider
import org.cgnal.graphe.tinkerpop.titan.{ TitanGraphProvider, EnrichedTitanManagement }
import org.cgnal.graphe.application.config.{CrossSellApplicationConfigReader, CrossSellApplicationConfig}

sealed class CrossSellApplication(protected val sparkContext: SparkContext,
                                  val config: CrossSellApplicationConfig) extends Application with SparkContextInstance {

  private def loadData() = Try {
    sparkContext.textFile(config.inputFileLocation).collect {
      case line if !line.startsWith("#") => CrossSellItems.fromString(line)
    }.collect {
      case Some(item) => item
    }.persist("Input")
  }

  private def loadVertices(rdd: RDD[CrossSellItems]) = Try {
    rdd.flatMap { crossSell => Seq(
      crossSell.productId.toLong -> Item(crossSell.productId),
      crossSell.crossSellProductId.toLong -> Item(crossSell.crossSellProductId))
    }.distinct().persist("Vertices")
  }

  private def loadEdges(rdd: RDD[CrossSellItems]) = Try {
    rdd.map { crossSell =>
      Edge(crossSell.productId, crossSell.crossSellProductId, crossSell)
    }.persist("Edges")
  }

  private def createTitanSchema() = Try {
    TitanGraphProvider.withGraphManagement { m =>
      m.createVertexLabel("Item") { _.partition() }

      m.createEdgeLabel("CrossSellItems") { _.multiplicity(MULTI) }

      m.createPropertyKey[JavaInt]("productId")   { _.cardinality(SINGLE) }
      m.createPropertyKey[JavaInt]("crossSellId") { _.cardinality(SINGLE) }
    }
  }

  private def graphX(vertices: RDD[(VertexId, Item)], edges: RDD[Edge[CrossSellItems]]) = Try { Graph(vertices, edges) }

  private def loadGraph()(implicit provider: NativeTinkerGraphProvider) = Try {
    sparkContext.loadNative[Item, CrossSellItems]
  }

  private def saveGraph(graph: Graph[Item, CrossSellItems])(implicit provider: NativeTinkerGraphProvider) = Try {
    graph.saveNativeGraph(false)
  }

  private def save() = for {
    data     <- timed("Loading data")        { loadData()          }
    vertices <- timed("Generating Vertices") { loadVertices(data)  }
    edges    <- timed("Generting Edges")     { loadEdges(data)     }
    _        <- timed("Showing Vertices")    { show(vertices)      }
    graph    <- graphX(vertices, edges)
    _        <- timed("Generating Schema")   { createTitanSchema() }
    _        <- timed("Saving Graph")        { saveGraph(graph)    }
  } yield ()

  private def load() = for {
    graph <- timed("Loading graph")       { loadGraph() }
    _     <- timed("Recovering Vertices") { show(graph.vertices.values) }
    _     <- timed("Recovering Edges")    { show(graph.edges.map { _.attr }) }
  } yield ()

  private def tearDown() = if (config.tearDown) {
    TitanGraphProvider.clearGraph() match {
      case Success(_) => log.info("Instance successfuly torn down")
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

object CrossSellApplication extends SparkContextProvider {

  def create(args: Seq[String]) = CrossSellApplicationConfigReader.withConfig(args) { conf =>
    withSparkContext("CrossSell", conf) { sparkContext => new CrossSellApplication(sparkContext, conf) }
  }

}
