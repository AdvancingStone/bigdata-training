package com.bluehonour.utils

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}

import scala.collection.JavaConversions._



object JsonUtils {

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

}
