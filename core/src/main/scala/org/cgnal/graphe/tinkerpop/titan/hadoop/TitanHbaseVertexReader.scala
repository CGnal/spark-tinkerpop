package org.cgnal.graphe.tinkerpop.titan.hadoop

import com.thinkaurelius.titan.core.RelationType
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType
import com.thinkaurelius.titan.graphdb.relations.RelationCache

import scala.collection.convert.decorateAsScala._

import com.thinkaurelius.titan.diskstorage.{ Entry, StaticBuffer }
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetup
import org.apache.tinkerpop.gremlin.structure._
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph
import org.slf4j.LoggerFactory
import com.thinkaurelius.titan.core.Cardinality._

class TitanHbaseVertexReader(titanHadoopSetup: TitanHadoopSetup) extends AutoCloseable {

  private lazy val idManager          = titanHadoopSetup.getIDManager
  private lazy val typeManager        = titanHadoopSetup.getTypeInspector
  private lazy val systemTypesManager = titanHadoopSetup.getSystemTypeInspector

  private val log = LoggerFactory.getLogger("cgnal.TitanHbaseVertexReader")

  private def nextOption[A](iterator: Iterator[A]) = if (iterator.hasNext) Some { iterator.next() } else None

  private def withGraph[A](f: Graph => A) = f { TinkerGraph.open() }

  private def withRelation[A](id: Long, entry: Entry)(f: RelationCache => A) = f {
    titanHadoopSetup.getRelationReader(id).parseRelation(entry, false, typeManager)
  }

  private def findVertex(vertexId: Long, graph: Graph, relation: RelationCache) = {
    if (systemTypesManager.isVertexLabelSystemType(relation.typeId)) Some {
      getOrCreateVertex(vertexId, graph, typeManager.getExistingVertexLabel(relation.getOtherVertexId).name)
    } else None
  }

  private def whenRegularType[A](relation: RelationCache) =
    if (
      systemTypesManager.isSystemType(relation.typeId) ||
      typeManager.getExistingRelationType(relation.typeId).asInstanceOf[InternalRelationType].isInvisibleType
    ) None else Some { relation }

  private def getOrCreateVertex(id: Long, graph: Graph, label: String = "") = nextOption { graph.vertices(Long.box(id)).asScala } match {
    case None if label.isEmpty => graph.addVertex(T.id, Long.box(id))
    case None                  => graph.addVertex(T.id, Long.box(id), T.label, label)
    case Some(vertex)          => vertex
  }

  private def getPropertyKeyCardinality(name: String) = {
    val relationType = typeManager.getRelationType(name)
    if (relationType == null || !relationType.isPropertyKey) VertexProperty.Cardinality.single
    else typeManager.getExistingPropertyKey(relationType.longId()).cardinality() match {
      case SINGLE => VertexProperty.Cardinality.single
      case SET    => VertexProperty.Cardinality.set
      case LIST   => VertexProperty.Cardinality.list
    }
  }

  private def addProperty(vertex: Vertex, relation: RelationCache, relationType: RelationType) = Some {
    vertex.property(
      getPropertyKeyCardinality(relationType.name),
      relationType.name,
      relation.getValue,
      T.id,
      Long.box(relation.relationId)
    )
  }

  private def hasRelation(vertex1: Vertex, vertex2: Vertex, relation: RelationCache) = (vertex1 == vertex2) && vertex1.edges(Direction.OUT).asScala.exists { _.id == relation.relationId }

  private def addEdge(vertex: Vertex, relation: RelationCache, relationType: RelationType, graph: Graph) = {
    if (idManager.isPartitionedVertex(relation.getOtherVertexId)) None
    else {
      val otherVertex = getOrCreateVertex(relation.getOtherVertexId, graph)
      if (!hasRelation(vertex, otherVertex, relation)) {
        if      (relation.direction == Direction.IN)  Some { otherVertex.addEdge(relationType.name, vertex, T.id, Long.box(relation.relationId)) }
        else if (relation.direction == Direction.OUT) Some { vertex.addEdge(relationType.name, otherVertex, T.id, Long.box(relation.relationId)) }
        else None
      } else {
        log.info { s"Found loop in vertex with id [${vertex.id()}] - using first matching relation" }
        vertex.edges(Direction.BOTH).asScala.find { _.id() == relation.relationId }
      }
    }
  }

  private def addEdgeProperties(edge: Edge, relation: RelationCache) = relation.asScala.foldLeft(edge) { (e, rel) =>
    val relationType = typeManager.getExistingRelationType(rel.key)
    if (relationType.isPropertyKey) e.property(relationType.name(), rel.value)
    edge
  }

  private def getRelationVertex(vertexId: Long, entries: Iterable[Entry], graph: Graph) = entries.toStream.flatMap {
    withRelation(vertexId, _) { findVertex(vertexId, graph, _) }
  }.headOption getOrElse getOrCreateVertex(vertexId, graph)

  def readVertex(vertexId: Long, entries: Iterable[Entry]): Vertex = withGraph { g =>
    val vertex = getRelationVertex(vertexId, entries, g)

    entries.foreach {
      withRelation(vertexId, _) { whenRegularType }.foreach { relation =>
        val relationType = typeManager.getExistingRelationType(relation.typeId)

        if (relationType.isPropertyKey) addProperty(vertex, relation, relationType)
        else addEdge(vertex, relation, relationType, g).map { addEdgeProperties(_, relation) }
      }
    }

    vertex
  }

  def readVertex(key: StaticBuffer, entries: Iterable[Entry]): Vertex = readVertex(idManager.getKeyID(key), entries)

  def close() = titanHadoopSetup.close()

}
