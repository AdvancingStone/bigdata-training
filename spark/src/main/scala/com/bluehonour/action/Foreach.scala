package com.bluehonour.action

import com.bluehonour.SparkContext

object Foreach extends SparkContext with App {

  val c = sc.parallelize(List("cat", "dog", "tiger", "lion", "gnu", "crocodile", "ant", "whale", "dolphin", "spider"), 3)
  c.foreach(x => println(x + "s are yummy"))
  //lions are yummy
  //cats are yummy
  //dogs are yummy
  //tigers are yummy
  //ants are yummy
  //whales are yummy
  //dolphins are yummy
  //spiders are yummy
  //gnus are yummy
  //crocodiles are yummy
}
