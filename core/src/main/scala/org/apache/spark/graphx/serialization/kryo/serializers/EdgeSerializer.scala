package org.apache.spark.graphx.serialization.kryo.serializers

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.apache.spark.graphx.Edge

object EdgeSerializer extends Serializer[Edge[Any]] {

  def write(kryo: Kryo, output: Output, edge: Edge[Any]) = {
    output.writeLong(edge.srcId)
    output.writeLong(edge.dstId)
    kryo.writeClassAndObject(output, edge.attr)
  }

  def read(kryo: Kryo, input: Input, edge: Class[Edge[Any]]) = Edge[Any](
    input.readLong(),
    input.readLong(),
    kryo.readClassAndObject(input)
  )

}
