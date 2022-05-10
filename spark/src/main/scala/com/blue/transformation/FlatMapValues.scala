package com.blue.transformation

import com.blue.SparkContext

/**
 * Very similar to mapValues, but collapses the inherent structure of the values during mapping.
 */
object FlatMapValues extends SparkContext with App {

  val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  val b = a.map(x => (x.length, x))
  println(b.collect.mkString(","))
  //(3,dog),(5,tiger),(4,lion),(3,cat),(7,panther),(5,eagle)

  val result: Array[(Int, Char)] = b.flatMapValues("[" + _ + "]").collect
  println(result.mkString(","))
  // (3,[),(3,d),(3,o),(3,g),(3,]),
  // (5,[),(5,t),(5,i),(5,g),(5,e),(5,r),(5,]),
  // (4,[),(4,l),(4,i),(4,o),(4,n),(4,]),
  // (3,[),(3,c),(3,a),(3,t),(3,]),
  // (7,[),(7,p),(7,a),(7,n),(7,t),(7,h),(7,e),(7,r),(7,]),
  // (5,[),(5,e),(5,a),(5,g),(5,l),(5,e),(5,])

  val result2: Array[(Int, Char)] = b.flatMapValues(x => x.toString).collect
  println(result2.mkString(","))
  //(3,d),(3,o),(3,g),(5,t),(5,i),(5,g),(5,e),(5,r),(4,l),(4,i),(4,o),(4,n),(3,c),(3,a),(3,t),(7,p),(7,a),(7,n),(7,t),(7,h),(7,e),(7,r),(5,e),(5,a),(5,g),(5,l),(5,e)

}
