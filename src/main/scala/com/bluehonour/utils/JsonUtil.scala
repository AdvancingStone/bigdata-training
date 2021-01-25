package com.bluehonour.utils

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.bluehonour.constant.JsonConstant
import com.bluehonour.utils.YamlUtil.EmbeddedField

import scala.collection.JavaConversions._

object JsonUtil {

  def removeElem(jsonObj: JSONObject, name: String): Boolean = {
    val value = jsonObj.remove(name)
    if (value != null) true else false
  }

  def removeElemGetValue(jsonObj: JSONObject, name: String): String = {
    val value = jsonObj.remove(name).toString
    value
  }

  def traverseJsonElems(jsonObj: JSONObject): util.ArrayList[(String, String)] = {
    val listResult = new util.ArrayList[(String, String)]()
    val iterator = jsonObj.iterator
    while (iterator.hasNext) {
      val tuple: (String, AnyRef) = iterator.next()
      listResult.add((tuple._1, tuple._2.toString))
    }
    listResult
  }

  def traverseJsonElems(jsonStr: String, name: String): List[(String, String)] = {
    var listResult: List[(String, String)] = List()
    val jsonObj = JSON.parseObject(jsonStr)
    val value: String = jsonObj.get(name).toString
    val obj = JSON.parseObject(value)
    val iterator = obj.iterator
    while (iterator.hasNext) {
      val tuple: (String, AnyRef) = iterator.next()
      listResult = listResult :+ (tuple._1, tuple._2.toString)
    }
    listResult
  }

  def traverseJsonElems(jsonStr: String, names: List[String]): util.HashMap[String, List[(String, String)]] = {
    val map = new util.HashMap[String, List[(String, String)]]()
    val jsonObj = JSON.parseObject(jsonStr)
    for (name <- names) {
      var list: List[(String, String)] = List()
      val value: String = jsonObj.get(name).toString
      if (value.trim.startsWith("{")) {
        val obj = JSON.parseObject(value)
        val iterator = obj.iterator
        while (iterator.hasNext) {
          val tuple: (String, AnyRef) = iterator.next()
          list = list :+ (tuple._1, tuple._2.toString)
        }
      }
      if (!list.isEmpty) {
        map.put(name, list)
      }
    }
    map
  }

  def combineJson(jsonObj: JSONObject, parentElem: String): util.HashMap[String, Object] = {
    val tuples: util.ArrayList[(String, String)] = traverseJsonElems(jsonObj)
    val map = new util.HashMap[String, Object]
    for (tuple: (String, String) <- tuples) {
      val combineParent: String = StringUtil.combineString(parentElem, tuple._1)
      val child: String = tuple._2
      var childObj: Object = child
      if (child.trim.startsWith("[")) {
        childObj = JSON.parseArray(child)
      } else if (child.trim.startsWith("{")) {
        childObj = JSON.parseObject(child)
      }
      map.put(combineParent, childObj)
    }
    map
  }

  def combineJson(jsonStr: String, names: List[String]): List[String] = {
    var listResult: List[String] = List()
    val jsonObj = JSON.parseObject(jsonStr)
    for (name <- names) {
      val value: String = jsonObj.get(name).toString.trim
      if (value.startsWith("{")) {
        val obj = JSON.parseObject(value)
        val iterator = obj.iterator
        while (iterator.hasNext) {
          val tuple = iterator.next()
          listResult = listResult :+ StringUtil.combineString(name, tuple._1)
        }
      }
    }
    listResult
  }

  def multiFieldsParse(jsonStr: String, items: List[String]): String = {
    var jsonResult = jsonStr
    for (item <- items) {
      val json: JSONObject = JSON.parseObject(jsonResult)
      val result: String = removeElemGetValue(json, item)
      if(result.trim.startsWith("{")){
        val combineJsonResult: util.HashMap[String, Object] = combineJson(JSON.parseObject(result), item)
        json.fluentPutAll(combineJsonResult).toJSONString
        jsonResult = json.toJSONString
      }
    }
    jsonResult
  }

  def embeddedFieldsParse(jsonStr: String, items: List[EmbeddedField]): String = {
    var jsonResult = jsonStr
    var list: List[String] = List()
    for (item <- items) {
      var tmpItems = List(item.field)
      for (i <- 0 until item.tier) {
        val tmpJson: String = multiFieldsParse(jsonResult, tmpItems)
        list = combineJson(jsonResult, tmpItems)
        jsonResult = tmpJson
        tmpItems = list
      }
    }
    jsonResult
  }

  def multiFieldsJson(jsonStr: String): String = {
    val items: List[String] = YamlUtil.getMultiFieldsValue(JsonConstant.MULTI_FIELDS)
    val result: String = JsonUtil.multiFieldsParse(jsonStr, items)
    result
  }

  def embededFieldsJson(jsonStr: String): String = {
    val items: List[YamlUtil.EmbeddedField] = YamlUtil.getEmbeddedFieldsValue(JsonConstant.EMBEDDED_FIELDS)
    val result: String = JsonUtil.embeddedFieldsParse(jsonStr, items)
    result
  }

}
