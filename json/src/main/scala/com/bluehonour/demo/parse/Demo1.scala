package com.bluehonour.demo.parse

import java.io.{FileOutputStream, PrintStream}

import com.alibaba.fastjson.{JSON, JSONArray}
import com.bluehonour.demo.utils.CloseUtil

import scala.collection.JavaConversions._
import scala.io.Source

object Demo1 {

  val outputFile = "D:\\home\\xxx\\tmp\\config.csv"
  val inputFile = this.getClass.getClassLoader.getResource("config.txt").getPath

  def main(args: Array[String]): Unit = {

    val json = Source.fromFile(inputFile, "UTF-8").mkString
    val jsonArray: JSONArray = JSON.parseArray(json)

    CloseUtil.using(new PrintStream(new FileOutputStream(outputFile), true, "UTF-8")) {
      ps => {
        jsonArray.foreach(json => {
          val obj = JSON.parseObject(json.toString)
          val tagName = obj.get("tagName").toString.trim
          val tagId = "W" + obj.get("tagId").toString.trim
          val level1 = "\"" + tagName + "\",\"" + tagId + "\",\"W001\",\"0\""
          println(level1)
          ps.println(level1)
          val tagAttrValues = obj.get("tagAttrValues").toString
          val attrArray = JSON.parseArray(tagAttrValues)
          attrArray.foreach(value => {
            val tmpObj = JSON.parseObject(value.toString)
            val attrId = "W" + tmpObj.get("attrId")
            val text = tmpObj.get("text")
            val level2 = "\"" + text + "\",\"" + attrId + "\",\"" + tagId + "\",\"1\""
            println(level2)
            ps.println(level2)
          })
        })
      }
    }
  }

}
