package org.apache.spark.graphx.serialization.kryo.serializers

import org.apache.spark.graphx.impl.EdgePartition

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }
import org.apache.spark.graphx.io.KryoGraphIO

object EdgePartitionSerializer extends Serializer[EdgePartition[Any, Any]] {

  def write(kryo: Kryo, output: Output, partition: EdgePartition[Any, Any]) = KryoGraphIO.writeAsJava(kryo, output, partition)

  def read(kryo: Kryo, input: Input, partition: Class[EdgePartition[Any, Any]]) = KryoGraphIO.readAsJava(kryo, input, partition)

}