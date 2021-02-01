package com.bluehonour

import com.bluehonour.transformation.SparkContext
import org.apache.spark.rdd.RDD

/**
 * Will create a checkpoint when the RDD is computed next.
 * Checkpointed RDDs are stored as a binary file within the checkpoint directory which can be specified using the Spark context.
 * (Warning: Spark applies lazy evaluation. Checkpointing will not occur until an action is invoked.)
 *
 * Important note: the directory  "my_directory_name" should exist in all slaves. As an alternative you could use an HDFS directory URL as well.
 */
object Checkpoint extends SparkContext with App {
  val my_directory_name = "D:\\home\\checkpoint"
  sc.setCheckpointDir(my_directory_name)
  val a: RDD[Int] = sc.parallelize(1 to 5)
  a.checkpoint()
  val value: Long = a.count()
  println(value)
  // 5
}
