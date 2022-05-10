package com.blue.interceptors

import java.util

import org.apache.kafka.clients.producer.{ProducerInterceptor, ProducerRecord, RecordMetadata}

class UserDefineProducerInterceptor extends ProducerInterceptor[Object, Object]{
  override def onSend(record: ProducerRecord[Object, Object]): ProducerRecord[Object, Object] = {

    return new ProducerRecord[Object, Object](record.topic(), record.key(), record.value()+"...userdefine")
  }

  override def onAcknowledgement(metadata: RecordMetadata, exception: Exception): Unit = {
    println(s"metadata: ${metadata}\texception: ${exception}")
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}
