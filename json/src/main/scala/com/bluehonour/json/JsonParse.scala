package com.bluehonour.json

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.io.Source

object JsonParse {

  var fileName: String = _
  var path: String = _
  var json: JSONObject = _
  var tmpObj: Object = _

  def main(args: Array[String]): Unit = {
    fileName = "test2.json"
    path = this.getClass.getClassLoader.getResource(fileName).getPath
    val jsonStr = Source.fromFile(path).mkString

    json = JSON.parseObject(jsonStr)
//    println(json)

    val array: JSONArray = JSON.parseArray(JSON.parseObject(json.get("data").toString).get("collectionlist").toString)
    println(array)


  }
}
