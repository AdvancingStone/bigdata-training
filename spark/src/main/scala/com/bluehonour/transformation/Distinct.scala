package com.bluehonour.transformation

import com.bluehonour.SparkContext

object Distinct extends SparkContext with App {

  val c = sc.parallelize(List("Gnu", "Cat", "Rat", "Dog", "Gnu", "Rat"), 2)
  val result: Array[String] = c.distinct.collect
  println(result.mkString(","))
  //  Dog,Cat,Gnu,Rat

  val a = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 1, 2, 3, 4, 5))
  println(a.partitions.length) //8

  val len = a.distinct(2).partitions.length
  println(len) //2

  val len2 = a.distinct(3).partitions.length
  println(len2) //3
}
