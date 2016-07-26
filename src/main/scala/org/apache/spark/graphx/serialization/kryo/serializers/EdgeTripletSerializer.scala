package org.apache.spark.graphx.serialization.kryo.serializers

import org.apache.spark.graphx.io.KryoGraphIO
import org.apache.spark.graphx.EdgeTriplet

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }

object EdgeTripletSerializer extends Serializer[EdgeTriplet[Any, Any]] {

  private def withNewTriplet[U](f: EdgeTriplet[Any, Any] => U) = f { new EdgeTriplet[Any, Any] }

  def write(kryo: Kryo, output: Output, triplet: EdgeTriplet[Any, Any]) = KryoGraphIO.writeAsJava(kryo, output, triplet)

  def read(kryo: Kryo, input: Input, tripletClass: Class[EdgeTriplet[Any, Any]]) = KryoGraphIO.readAsJava(kryo, input, tripletClass)

}
