package com.blue.transformation

import com.blue.SparkContext

object IsCheckpointed extends SparkContext with App {

  sc.setCheckpointDir("/home/cloudera/Documents")
  val c = sc.parallelize(1 to 10)
  println(c.isCheckpointed) //false

  c.checkpoint()
  println(c.isCheckpointed) //false

  c.collect()
  println(c.isCheckpointed) //true
}
