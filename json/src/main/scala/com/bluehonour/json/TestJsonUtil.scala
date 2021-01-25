package com.bluehonour.json

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluehonour.constant.JsonConstant
import com.bluehonour.utils.{JsonUtil, LoadConfigurationFile, YamlUtil}
import org.junit.{Before, Test}

import scala.io.Source

class TestJsonUtil {

  var fileName: String = _
  var path: String = _
  var json: JSONObject = _
  var jsonStr: String = _

  @Before
  def setUp: Unit = {
    fileName = "test2.json"
    path = this.getClass.getClassLoader.getResource(fileName).getPath
    jsonStr = Source.fromFile(path).mkString
  }

  @Test
  def testMultiFieldsParse(): Unit = {
    val items = YamlUtil.getMultiFieldsValue("multi_fields")
    val result = JsonUtil.multiFieldsParse(jsonStr, items)
    println(result)
  }

  @Test
  def testTraverseJsonElems(): Unit = {
    val data: List[(String, String)] = JsonUtil.traverseJsonElems(jsonStr, "data")
    println(data)
  }

  @Test
  def testCleanMap(): Unit = {
    val value = jsonStr
    val items: List[String] = YamlUtil.getMultiFieldsValue("multi_fields")
    val result: String = JsonUtil.multiFieldsParse(value, items)
    val json: JSONObject = JSON.parseObject(result)
    json.fluentPut("processTime", System.currentTimeMillis().toString).toJSONString
    val newValue = json.toJSONString
    println(newValue)
  }

  @Test
  def testTraverseJsonElems2(): Unit = {
    val list = List("code", "data", "data3", "data2")
    import scala.collection.JavaConversions._
    val map = JsonUtil.traverseJsonElems(jsonStr, list)
    val iterator = map.iterator
    while (iterator.hasNext) {
      val next = iterator.next()
      val name = next._1
      val value = next._2
      println(name, value)
    }
  }

  @Test
  def testEmbeddedFieldsParse(): Unit = {
    val items = YamlUtil.getEmbeddedFieldsValue("embedded_fields")
    val result = JsonUtil.embeddedFieldsParse(jsonStr, items)
    println(result)
  }

  @Test
  def testCombineJson(): Unit = {
    val list = List("data", "data2")
    val value = JsonUtil.combineJson(jsonStr, list)
    println(value)
  }

  @Test
  def testSetJsonParseStrategy(): Unit = {
    println(YamlUtil.getJsonParseStrategy())
    YamlUtil.setJsonParseStrategy(JsonConstant.EMBEDDED_FIELDS)
    println(YamlUtil.getJsonParseStrategy())
  }

  @Test
  def test(): Unit = {
    println(YamlUtil.strategy)
    val init = YamlUtil.getFieldsValue(JsonConstant.EMBEDDED_FIELDS)
    println(YamlUtil.strategy)
    val a = YamlUtil.getMultiFieldsValue(JsonConstant.MULTI_FIELDS)
    println(YamlUtil.strategy, a)
    val fields = YamlUtil.getEmbeddedFieldsValue(JsonConstant.EMBEDDED_FIELDS)
    println(YamlUtil.strategy, fields)
    YamlUtil.setJsonParseStrategy(JsonConstant.MULTI_FIELDS)
    println("222222222222222", YamlUtil.strategy)
  }

  @Test
  def testLoadJsonYaml(): Unit = {
    val value: util.Map[String, AnyRef] = LoadConfigurationFile.loadJsonYaml()
    println(value)
    val map: util.HashMap[String, Object] = value.get("json").asInstanceOf[util.HashMap[String, Object]]
    println(map)
  }
}
