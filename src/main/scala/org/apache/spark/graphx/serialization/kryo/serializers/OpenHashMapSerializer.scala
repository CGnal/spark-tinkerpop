package org.apache.spark.graphx.serialization.kryo.serializers

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }

import org.apache.spark.graphx.util.collection.{ GraphXPrimitiveKeyOpenHashMap => OpenHashMap }

object OpenHashMapSerializer extends Serializer[OpenHashMap[Any, Any]] {

  private def withNewHashMap[U](f: OpenHashMap[Any, Any] => U): OpenHashMap[Any, Any] = {
    val map = new OpenHashMap[Any, Any]()
    f { map }
    map
  }

  def write(kryo: Kryo, output: Output, hashMap: OpenHashMap[Any, Any]) = kryo.writeClassAndObject(output, hashMap.toArray[(Any, Any)])

  def read(kryo: Kryo, input: Input, hashSetClass: Class[OpenHashMap[Any, Any]]) = withNewHashMap { map =>
    kryo.readClassAndObject { input }.asInstanceOf[Array[(Any, Any)]].foreach { case (k, v) => map.update(k, v) }
  }

}
