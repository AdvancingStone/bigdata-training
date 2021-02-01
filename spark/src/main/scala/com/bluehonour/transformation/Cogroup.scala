package com.bluehonour.transformation

import com.bluehonour.SparkContext

/**
 * A very powerful set of functions that allow grouping up to 3 key-value RDDs together using their keys.
 */
object Cogroup extends SparkContext with App {

  val a = sc.parallelize(List(1, 2, 1, 3), 1)
  val b = a.map((_, "b"))
  val c = a.map((_, "c"))
  val d = a.map((_, "d"))

  val res: Array[(Int, (Iterable[String], Iterable[String]))] = b.cogroup(c).collect
  println(res.mkString(","))
  //  (1,(CompactBuffer(b, b),CompactBuffer(c, c))),
  //  (3,(CompactBuffer(b),CompactBuffer(c))),
  //  (2,(CompactBuffer(b),CompactBuffer(c)))

  val reuslt: Array[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = b.cogroup(c, d).collect
  println(reuslt.mkString(","))
  //  (1,(CompactBuffer(b, b),CompactBuffer(c, c),CompactBuffer(d, d))),
  //  (3,(CompactBuffer(b),CompactBuffer(c),CompactBuffer(d))),
  //  (2,(CompactBuffer(b),CompactBuffer(c),CompactBuffer(d)))

  val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
  val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
  val resArr: Array[(Int, (Iterable[String], Iterable[String]))] = x.cogroup(y).collect
  println(resArr.mkString(","))
  //  (4,(CompactBuffer(kiwi),CompactBuffer(iPad))),
  //  (2,(CompactBuffer(banana),CompactBuffer())),
  //  (1,(CompactBuffer(apple),CompactBuffer(laptop, desktop))),
  //  (3,(CompactBuffer(orange),CompactBuffer())),
  //  (5,(CompactBuffer(),CompactBuffer(computer)))

  val z = sc.parallelize(List((6, "banana"), (1, "laptop")), 2)
  val collect: Array[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = x.groupWith(y, z).collect
  println(collect.mkString(","))
//  (4,(CompactBuffer(kiwi),CompactBuffer(iPad),CompactBuffer())),
//  (6,(CompactBuffer(),CompactBuffer(),CompactBuffer(bana))),
//  (2,(CompactBuffer(banana),CompactBuffer(),CompactBuffer())),
//  (1,(CompactBuffer(apple),CompactBuffer(laptop, desktop),CompactBuffer(laptop))),
//  (3,(CompactBuffer(orange),CompactBuffer(),CompactBuffer())),
//  (5,(CompactBuffer(),CompactBuffer(computer),CompactBuffer()))
}
