package com.blue.util

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import java.util.Properties

class KafkaUtil[K, V] extends Serializable {

//  @transient
  private var producer: KafkaProducer[K, V] = _

  def this(props: Properties) {
    this()
    producer = new KafkaProducer[K, V](props)
  }

  def getKafkaProducer(props: Properties): KafkaProducer[K, V] = {
    if (producer == null) {
      producer = new KafkaProducer[K, V](props)
    }
    producer
  }

  /**
   * 同步发送
   *
   * @param topic
   * @param key
   * @param value
   */
  def sendMessagesSync(topic: String, key: K, value: V): Unit = {
    val record: ProducerRecord[K, V] = new ProducerRecord[K, V](topic, key, value)
    producer.send(record).get()
  }

  /**
   * 异步发送
   *
   * @param topic
   * @param key
   * @param value
   */
  def sendMessagesAsync(topic: String, key: K, value: V): Unit = {
    val record: ProducerRecord[K, V] = new ProducerRecord[K, V](topic, key, value)
    producer.send(record)
  }

  /**
   * 含回调函数
   * @param topic
   * @param key
   * @param value
   */
  def sendMessagesCallback(topic: String, key: K, value: V): Unit = {
    val record: ProducerRecord[K, V] = new ProducerRecord[K, V](topic, key, value)
    producer.send(record, new Callback {
      override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
        if (exception == null) {
          println(s"Successfully received the details as: Topic->${metadata.topic}\tPartition->${metadata.partition}\tOffset->${metadata.offset}\tTimestamp->${metadata.timestamp}")
        } else {
          println(s"Can't produce, getting error: $exception")
        }
      }
    }).get()
  }

  def closeKafkaProducer(): Unit = {
    if (producer != null) producer.close()
  }
}
