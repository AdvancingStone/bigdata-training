package com.bluehonour.json

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluehonour.utils.JsonUtils

import scala.io.Source

object Test2 {
  var fileName: String = _
  var path: String = _
  var json: JSONObject = _
  def main(args: Array[String]): Unit = {
    fileName = "test2.json"
    path = this.getClass.getClassLoader.getResource(fileName).getPath
    val jsonStr = Source.fromFile(path).mkString
    try {
      val json: JSONObject = JSON.parseObject(jsonStr)
      val data = "data"
      val result: String = JsonUtils.removeElemGetValue(json, data)

      val combineJsonResult: util.HashMap[String, Object] = JsonUtils.combineJson(JSON.parseObject(result), data)
      json.fluentPutAll(combineJsonResult)
        .fluentPut("processTime", System.currentTimeMillis().toString).toJSONString

      val newValue = json.toJSONString
      println(newValue)
      newValue
    } catch {
      case _: Exception => {
        jsonStr
      }
    }
  }

}
