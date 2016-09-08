package org.apache.spark.graphx.serialization.kryo.serializers

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}
import org.cgnal.common.domain.{Female, Gender, Male, User}

object UserSerializer extends Serializer[User] {

  private def writeGender(output: Output, gender: Gender) = gender match {
    case Male   => output.writeString { "male"   }
    case Female => output.writeString { "female" }
  }

  private def readGender(input: Input) = input.readString() match {
    case "male"   => Male
    case "female" => Female
    case other    => throw new IllegalArgumentException(s"Invalid string for gender [$other]")
  }

  def write(kryo: Kryo, output: Output, user: User) = {
    output.writeLong(user.id)
    output.writeString(user.name)
    writeGender(output, user.gender)
  }

  def read(kryo: Kryo, input: Input, userClass: Class[User]) = User(
    id     = input.readLong(),
    name   = input.readString(),
    gender = readGender(input)
  )

}
