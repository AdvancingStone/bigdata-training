package com.bluehonour.demo.parse

import java.io.{File, FileOutputStream, PrintStream}

import com.alibaba.fastjson.JSON
import com.bluehonour.demo.utils.{AES, CloseUtil, ZipUtil}

import scala.collection.JavaConversions._
import scala.io.{BufferedSource, Source}

object Demo2 {
  val map: Map[String, String] =
    Map("0" -> "aaa",
      "1" -> "bbb",
      "4" -> "ccc",
      "5" -> "ddd",
      "6" -> "eee")
  val outputFile = "D:\\home\\xxx\\tmp\\list.csv"
  val outputZipFile = "D:\\home\\xxx\\tmp\\list.csv.zip"
  val inputFile = this.getClass.getClassLoader.getResource("list.txt").getPath

  def main(args: Array[String]): Unit = {

    val source: BufferedSource = Source.fromFile(inputFile, "UTF-8")
    val lines = source.getLines().toList

    CloseUtil.using(new PrintStream(new FileOutputStream(outputFile), true, "UTF-8")) {
      ps => {

        lines.foreach(json => {
          val array = JSON.parseArray(json)
          array.foreach(jsonStr => {
            val jsonObj = JSON.parseObject(jsonStr.toString)
            val source = jsonObj.get("source").toString
            val id_type = map.get(source).get
            val value = AES.AESEncode(AES.ENCODE_RULES, jsonObj.get("value").toString)
            val taglist = jsonObj.get("taglist").toString
            val taglistArr = JSON.parseArray(taglist)
            var list: List[String] = List()
            taglistArr.foreach(tmpJsonStr => {
              val tmpJsonObj = JSON.parseObject(tmpJsonStr.toString)
              val attrId = "W" + tmpJsonObj.get("attrId")
              list = list :+ attrId
            })
            val result = "\"" + id_type + "\",\"" + value + "\",\"" + list.mkString(",") + "\""
            println(result)
            ps.println(result)
          })
        })
        println(s"start to compress file: list.csv, please wait...")
        CloseUtil.using(new FileOutputStream(new File(outputZipFile))) {
          outputStream => {
            ZipUtil.toZip(outputFile, outputStream, true)
          }
        }
      }
    }
  }
}
