package com.bluehonour.transformations

import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}

object Union {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val ds1 = env.fromCollection(List(("a", 1), ("b", 2), ("c", 3)))
    val ds2 = env.fromCollection(List(("d", 4), ("e", 5), ("f", 6)))
    val ds3 = env.fromCollection(List(("g",7),("h", 8)))
    val ds = ds1.union(ds2, ds3)
    ds.print()
    env.execute()



  }

}
