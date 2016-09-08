package org.apache.spark.graphx.io

import scala.reflect.ClassTag

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.serializers.JavaSerializer

import org.apache.hadoop.io.{ BytesWritable, NullWritable }

import org.apache.spark.SparkContext
import org.apache.spark.graphx.serialization.kryo.{ KryoSerde, KryoRegistry }
import org.apache.spark.rdd.RDD

/**
 * Provides functions to perform Kryo and Java serialization (using Kryo's `JavaSerializer`).
 */
object KryoGraphIO extends Serializable {

  private def createJavaSerializer(serializer: JavaSerializer = new JavaSerializer()) = {
    serializer.setAcceptsNull(true)
    serializer.setImmutable(true)
    serializer
  }

  def javaSerializer = createJavaSerializer()

  /**
   * Reads and de-serializes Kryo data grouped in chunks of 128.
   * @param sparkContext the `SparkContext` instance into which to load the resulting `RDD`
   * @param registry `KryoRegistry` instance containing all registered serializer classes
   * @param path path to the directory where to store the data file
   * @tparam AA the type to be serialized (note that this must be registered in the provided `registry`
   */
  def readGrouped[AA](sparkContext: SparkContext, registry: KryoRegistry)(path: String)(implicit AA: ClassTag[AA]) = sparkContext.sequenceFile(
    path,
    classOf[NullWritable],
    classOf[BytesWritable]
  ).flatMap { case (_, bytes) => KryoSerde.read[Array[AA]](registry)(bytes.getBytes) }

  /**
   * Serializes and writes Kryo data in chunks of 128 elements.
   * @param registry `KryoRegistry` instance containing all registered serializer classes
   * @param path path to the directory where to store the data file
   * @param rdd the rdd to store
   * @tparam AA the type to be serialized (note that this must be registered in the provided `registry`
   */
  def writeGrouped[AA](registry: KryoRegistry, path: String)(rdd: RDD[AA])(implicit AA: ClassTag[AA]) = rdd.mapPartitions {
    _ grouped 128 map { _.toArray[AA] }
  }.map { pairs => NullWritable.get -> KryoSerde.write(registry)(pairs) }.saveAsSequenceFile(path)

  def writeAsJava[A <: Serializable](kryo: Kryo, output: Output, a: A) = javaSerializer.write(kryo, output, a)

  def readAsJava[A <: Serializable](kryo: Kryo, input: Input, aClass: Class[A]) = javaSerializer.read(kryo, input, aClass).asInstanceOf[A]

}
