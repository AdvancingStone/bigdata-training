package com.bluehonour.others

import com.bluehonour.SparkContext

/**
 * Allows a RDD to be tagged with a custom name.
 *
 * Listing Variants
 * -   @transient var name: String
 * -   def setName(_name: String)
 */
object Name extends SparkContext with App {

  val y = sc.parallelize(1 to 10, 10)
  println(y.name) // null
  y.setName("Fancy RDD Name")
  println(y.name) //Fancy RDD Name

}
