package com.bluehonour.json

import java.io.BufferedReader

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.io.Source

object JsonUtil {
  var line: String = _
  var reader: BufferedReader = _
  var sb: StringBuilder = _

  def main(args: Array[String]): Unit = {
    val fileName = "test.json"
    val path = this.getClass.getClassLoader.getResource(fileName).getPath
    println(path)
    val jsonStr = Source.fromFile(path).mkString

    val json: JSONObject = JSON.parseObject(jsonStr)
    println(json)

    val pipeline = json.getString("pipeline")
    println(pipeline)

    val dataJson: JSONObject = JSON.parseObject(json.getString("data"))
    println(dataJson)

    val userInfo: JSONObject = JSON.parseObject(dataJson.getString("userInfo"))
    println(userInfo)

    val phoneNumber: String = userInfo.getString("phoneNumber")
    println(phoneNumber)

  }
}
