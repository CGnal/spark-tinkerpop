package org.apache.spark.graphx.serialization.kryo

import org.apache.spark.graphx.serialization.kryo.serializers.{ ConnectionSerializer, UserSerializer }

import org.cgnal.common.domain.{ Connection, User }

object TestKryoRegistry extends KryoRegistry with Serializable {

  protected def registry =
    { classOf[User]       serializeWith UserSerializer       } :+
    { classOf[Connection] serializeWith ConnectionSerializer }


}
