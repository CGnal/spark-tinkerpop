package org.apache.spark.graphx.serialization.kryo

import com.esotericsoftware.kryo.Serializer

import org.apache.spark.graphx.{ EdgeTriplet, Edge }
import org.apache.spark.graphx.impl.EdgePartition
import org.apache.spark.graphx.serialization.kryo.serializers._
import org.apache.spark.graphx.util.collection.{ GraphXPrimitiveKeyOpenHashMap => OpenHashMap }

sealed trait BaseKryoRegistry {

  final protected def baseRegistry: Seq[KryoRegistrable[_]] =
    { classOf[EdgeTriplet[Any, Any]]   serializeWith EdgeTripletSerializer   } :+
    { classOf[Edge[Any]]               serializeWith EdgeSerializer          } :+
    { classOf[OpenHashMap[Any, Any]]   serializeWith OpenHashMapSerializer   } :+
    { classOf[EdgePartition[Any, Any]] serializeWith EdgePartitionSerializer }
}

/**
 * Provides the list of classes and their serializers in the form of a sequence of `KryoRegistrable` instances, for
 * example:
 * {{{
 *   import org.apache.spark.graphx.serialization.kryo.ClassRegistrable
 *
 *   ...
 *
 *   final protected def registry: Seq[KryoRegistrable[_]] =
 *     { classOf[MyClass]      serializeWith MyClassSerializer      } :+
 *     { classOf[MyOtherClass] serializeWith MyOtherClassSerializer }
 * }}}
 *
 * Note that the basic Spark class serializers are already added by default.
 */
trait KryoRegistry extends BaseKryoRegistry { this: Serializable =>

  protected def registry: Seq[KryoRegistrable[_]]

  final def finalRegistry = registry ++ baseRegistry

}

final case class KryoRegistrable[A](aClass: Class[A], serializer: Serializer[A]) {

  def :+[B](other: KryoRegistrable[B]): Seq[KryoRegistrable[_]] = Seq(this, other)

}
