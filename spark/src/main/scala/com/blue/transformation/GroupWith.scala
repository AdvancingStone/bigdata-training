package com.blue.transformation

import com.blue.SparkContext
import com.blue.transformation.Cogroup.sc

object GroupWith extends SparkContext with App{
  val x = sc.parallelize(List((1, "apple"), (2, "banana"), (3, "orange"), (4, "kiwi")), 2)
  val y = sc.parallelize(List((5, "computer"), (1, "laptop"), (1, "desktop"), (4, "iPad")), 2)
  val z = sc.parallelize(List((6, "banana"), (1, "laptop")), 2)

  val result: Array[(Int, (Iterable[String], Iterable[String]))] = x.groupWith(y).collect
  println(result.mkString(","))
//  (4,(CompactBuffer(kiwi),CompactBuffer(iPad))),
//  (2,(CompactBuffer(banana),CompactBuffer())),
//  (1,(CompactBuffer(apple),CompactBuffer(laptop, desktop))),
//  (3,(CompactBuffer(orange),CompactBuffer())),
//  (5,(CompactBuffer(),CompactBuffer(computer)))

  val res: Array[(Int, (Iterable[String], Iterable[String], Iterable[String]))] = x.groupWith(y, z).collect
  println(res.mkString(","))
  //  (4,(CompactBuffer(kiwi),CompactBuffer(iPad),CompactBuffer())),
  //  (6,(CompactBuffer(),CompactBuffer(),CompactBuffer(bana))),
  //  (2,(CompactBuffer(banana),CompactBuffer(),CompactBuffer())),
  //  (1,(CompactBuffer(apple),CompactBuffer(laptop, desktop),CompactBuffer(laptop))),
  //  (3,(CompactBuffer(orange),CompactBuffer(),CompactBuffer())),
  //  (5,(CompactBuffer(),CompactBuffer(computer),CompactBuffer()))

}
