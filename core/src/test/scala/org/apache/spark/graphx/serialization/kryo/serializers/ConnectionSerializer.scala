package org.apache.spark.graphx.serialization.kryo.serializers

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.cgnal.common.domain.{Acquaintance, Connection, Friend, Knows, Relationship, Spouse}

object ConnectionSerializer extends Serializer[Connection] {

  private def writeRelationship(output: Output, relationship: Relationship) = relationship match {
    case Spouse       => output.writeString { "spouse"       }
    case Acquaintance => output.writeString { "acquaintance" }
    case Friend       => output.writeString { "friend"       }
  }

  private def readRelationship(input: Input) = input.readString() match {
    case "spouse"       => Spouse
    case "acquaintance" => Acquaintance
    case "friend"       => Friend
    case other          => throw new IllegalArgumentException(s"Invalid string for relationship [$other]")
  }

  private def writeKnows(output: Output, knows: Knows) = {
    output.writeString("knows")
    output.writeLong { knows.source }
    output.writeLong { knows.destination }
    writeRelationship(output, knows.relationship)
  }

  private def readKnows(input: Input) = Knows(
    source       = input.readLong(),
    destination  = input.readLong(),
    relationship = readRelationship(input)
  )

  def write(kryo: Kryo, output: Output, connection: Connection) = connection match {
    case knows: Knows => writeKnows(output, knows)
    case other        => throw new IllegalArgumentException(s"Invalid value for connection [$other]")
  }

  def read(kryo: Kryo, input: Input, connectionClass: Class[Connection]) = input.readString() match {
    case "knows" => readKnows(input)
    case other   => throw new IllegalArgumentException(s"Invalid type of connect encountered [$other]")
  }

}
