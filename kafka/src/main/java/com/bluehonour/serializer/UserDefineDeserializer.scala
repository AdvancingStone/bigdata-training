package com.bluehonour.serializer

import java.util

import org.apache.commons.lang3.SerializationUtils
import org.apache.kafka.common.serialization.Deserializer

class UserDefineDeserializer extends Deserializer[Object]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    println("deserializer configure...")
  }

  override def deserialize(topic: String, data: Array[Byte]): AnyRef = {
    return SerializationUtils.deserialize(data)
  }

  override def close(): Unit = {
    println("deserializer close...")
  }

}
