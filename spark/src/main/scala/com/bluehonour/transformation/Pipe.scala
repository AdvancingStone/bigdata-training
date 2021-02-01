package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Takes the RDD data of each partition and sends it via stdin to a shell-command.
 * The resulting output of the command is captured and returned as a RDD of string values.
 */
object Pipe extends SparkContext with App {
  val a = sc.parallelize(1 to 9, 3)
  val result: Array[String] = a.pipe("head -n 1").collect
  println(result.mkString(","))
  // 1,4,7
}
