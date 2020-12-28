package com.bluehonour.json

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluehonour.utils.JsonUtils

import scala.io.Source

object TestJsonUtils {
  var fileName: String = _
  var path: String = _
  var json: JSONObject = _
  def main(args: Array[String]): Unit = {
    fileName = "test2.json"
    path = this.getClass.getClassLoader.getResource(fileName).getPath
    val jsonStr = Source.fromFile(path).mkString

    json = JSON.parseObject(jsonStr)
    println(json)

    val data = "data"
    val result: String = JsonUtils.removeElemGetValue(json, data)
    println(s"result -> ${result}")

    println(s"now json -> ${json}")

    val combineJsonResult: util.HashMap[String, Object] = JsonUtils.combineJson(JSON.parseObject(result), data)
    println(s"----------${combineJsonResult}")

    println(json)

    json.fluentPutAll(combineJsonResult)
    println(json)

    val str = json.toString()//.replaceAll("\\\\", "")
    println(str)

  }

}
