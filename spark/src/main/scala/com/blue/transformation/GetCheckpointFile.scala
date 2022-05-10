package com.blue.transformation

import com.blue.SparkContext

/**
 * Returns the path to the checkpoint file or null if RDD has not yet been checkpointed.
 */
object GetCheckpointFile extends SparkContext with App {
  sc.setCheckpointDir("/home/cloudera/Documents")
  val a = sc.parallelize(1 to 500, 5)
  val b = a++a++a++a++a
  println(b.getCheckpointFile)  //None

  b.checkpoint
  println(b.getCheckpointFile)  //None

  b.collect
  println(b.getCheckpointFile)  //Some(file:/home/cloudera/Documents/762e5f41-eb3e-4d28-a13c-60b0337d3b47/rdd-4)
}
