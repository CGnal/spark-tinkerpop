package org.apache.spark.graphx.serialization.kryo

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{ Input, Output }

import com.twitter.chill.{ EmptyScalaKryoInstantiator, AllScalaRegistrar }

object KryoSerde extends Serializable {

  @transient private var kryoContext: Option[Kryo] = None

  private def setKryoContext(context: Kryo) = kryoContext = Option { context }

  private def makeKryoContext(registry: KryoRegistry,
                              kryo: Kryo = new EmptyScalaKryoInstantiator().newKryo()) = {
    new AllScalaRegistrar() apply kryo
    registry.finalRegistry.foreach { registrable => kryo.register(registrable.aClass, registrable.serializer) }
    kryo.setReferences  { false }
    kryo.setClassLoader { this.getClass.getClassLoader }
    setKryoContext      { kryo }
    kryo
  }

  def withKryoContext[A](registry: KryoRegistry)(f: Kryo => A) = kryoContext match {
    case Some(context) => f { context }
    case None          => f { makeKryoContext(registry) }
  }

  def write[A](registry: KryoRegistry)(a: A): Array[Byte] = withKryoContext(registry) { context =>
    val stream = new ByteArrayOutputStream(64)
    val output = new Output(stream)
    context.writeClassAndObject(output, a)
    output.close()
    stream.toByteArray
  }

  def read[A](registry: KryoRegistry)(buffer: Array[Byte]): A = withKryoContext(registry) { context =>
    val input = new Input(buffer)
    val a = context.readClassAndObject(input)
    input.close()
    a.asInstanceOf[A]
  }

}
