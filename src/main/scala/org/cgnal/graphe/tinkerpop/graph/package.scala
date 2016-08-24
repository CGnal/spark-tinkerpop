package org.cgnal.graphe.tinkerpop

import scala.reflect.ClassTag

import com.thinkaurelius.titan.core.schema.{ VertexLabelMaker => TitanVertexLabelMaker, EdgeLabelMaker => TitanEdgeLabelMaker, PropertyKeyMaker => TitanPropertyKeyMaker, TitanManagement }

package object graph {

  implicit class EnrichedTitanManagement(m: TitanManagement) {

    def createVertexLabel(name: String)(f: TitanVertexLabelMaker => TitanVertexLabelMaker): Unit =
      if (m.getVertexLabel(name) == null) f { m.makeVertexLabel(name) }.make()

    def createEdgeLabel(name: String)(f: TitanEdgeLabelMaker => TitanEdgeLabelMaker): Unit =
      if (m.getEdgeLabel(name) == null) f { m.makeEdgeLabel(name) }.make()

    def createPropertyKey[A](name: String)(f: TitanPropertyKeyMaker => TitanPropertyKeyMaker)(implicit A: ClassTag[A]): Unit =
      if (m.getPropertyKey(name) == null) f { m.makePropertyKey(name).dataType(A.runtimeClass.asInstanceOf[Class[A]]) }.make()

  }

}
