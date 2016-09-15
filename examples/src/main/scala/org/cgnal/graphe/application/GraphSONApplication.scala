package org.cgnal.graphe.application

import scala.util.{ Try, Success, Failure }

import org.apache.hadoop.fs.{ FileSystem, Path }

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import org.cgnal.graphe.application.config.{ ExampleApplicationConfigReader, ExampleApplicationConfig }
import org.cgnal.graphe.arrows.DomainArrows._
import org.cgnal.graphe.domain.{ Item, CrossSellItems }
import org.cgnal.graphe.{ GraphEGraph, GraphESparkContext }
import org.cgnal.graphe.tinkerpop.graph.TinkerGraphProvider
import org.cgnal.graphe.application.Providers.emptyGraphProvider

sealed class GraphSONApplication(protected val sparkContext: SparkContext,
                                 val config: ExampleApplicationConfig) extends Application with SparkContextInstance {

  private val outputDir  = "output-data"
  private val outputPath = new Path(outputDir)

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

  private def graphX(vertices: RDD[(VertexId, Item)], edges: RDD[Edge[CrossSellItems]]) = Try { Graph(vertices, edges) }

  private def loadGraph()(implicit provider: TinkerGraphProvider) = Try {
    sparkContext.load[Item, CrossSellItems](outputDir).asGraphSON
  }

  private def saveGraph(graph: Graph[Item, CrossSellItems])(implicit provider: TinkerGraphProvider) = Try {
    graph.save(outputDir).asGraphSON
  }

  private def save() = for {
    data     <- timed("Loading data")        { loadData()          }
    vertices <- timed("Generating Vertices") { loadVertices(data)  }
    edges    <- timed("Generting Edges")     { loadEdges(data)     }
    _        <- timed("Showing Vertices")    { show(vertices)      }
    graph    <- graphX(vertices, edges)
    _        <- timed("Saving Graph")        { saveGraph(graph)    }
  } yield ()

  private def load() = for {
    graph <- timed("Loading graph")       { loadGraph() }
    _     <- timed("Recovering Vertices") { show(graph.vertices.values) }
    _     <- timed("Recovering Edges")    { show(graph.edges.map { _.attr }) }
  } yield ()

  private def tearDown() = if (config.tearDown) {
    FileSystem.get { sparkContext.hadoopConfiguration }.delete(outputPath, true)
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

object GraphSONApplication extends SparkContextProvider {

  def create(args: Seq[String]) = ExampleApplicationConfigReader.withConfig(args) { conf =>
    withSparkContext("GraphSON", conf) { sparkContext => new GraphSONApplication(sparkContext, conf) }
  }

}
