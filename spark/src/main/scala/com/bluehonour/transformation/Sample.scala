package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * Randomly selects a fraction of the items of a RDD and returns them in a new RDD.
 */
object Sample extends SparkContext with App {
  val a = sc.parallelize(1 to 10000, 3)
  val res1: Long = a.sample(false, 0.1, 0).count()
  println(res1) //  1032

  val res2: Long = a.sample(true, 0.3, 0).count()
  println(res2) //  3110

  val res3: Long = a.sample(true, 0.3, 13).count
  println(res3) //  2952


}
