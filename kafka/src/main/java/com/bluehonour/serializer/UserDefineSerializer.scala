package com.bluehonour.serializer

import java.util

import org.apache.commons.lang3.SerializationUtils
import org.apache.kafka.common.serialization.Serializer

class UserDefineSerializer extends Serializer[Object]{
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {
    println("serializer configure...")
  }

  override def serialize(topic: String, data: Object): Array[Byte] = {
    return SerializationUtils.serialize(data.asInstanceOf[Serializable])
  }

  override def close(): Unit = {
    println("serializer clone...")
  }
}
