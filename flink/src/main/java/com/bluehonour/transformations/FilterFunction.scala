package com.bluehonour.transformations

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object FilterFunction {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.readTextFile("data/carFlow_all_column_test.txt")
    stream.filter(new FilterFunction[String]{
      override def filter(value: String): Boolean = {
        if(null != value && !"".equals(value)){
          val speed = value.split(",")(6).replace("'", "").toLong
          if(speed>1000){
            false
          }else{
            true
          }
        }else{
          false
        }
      }
    }).print()
    env.execute()
  }

}
