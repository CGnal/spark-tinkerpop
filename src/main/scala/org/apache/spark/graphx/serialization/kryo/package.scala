package org.apache.spark.graphx.serialization

import com.esotericsoftware.kryo.Serializer

package object kryo {

  implicit class ClassRegistrable[A](c: Class[A]) {

    def serializeWith(serializer: Serializer[A]) = KryoRegistrable(c, serializer)

    /**
     * alias for `serializeWith`
     */
    def :>:(serializer: Serializer[A]) = serializeWith(serializer)

  }

}
