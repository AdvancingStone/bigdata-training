package com.blue.action

import com.blue.SparkContext

/**
 * Saves the RDD as a Hadoop sequence file.
 */
object SaveAsSequenceFile extends SparkContext with App {

  val v = sc.parallelize(Array(("owl", 3), ("gnu", 4), ("dog", 1), ("cat", 2), ("ant", 5)), 2)
  v.saveAsSequenceFile("hd_seq_file")
}
