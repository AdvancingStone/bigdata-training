package com.bluehonour.utils

/**
 * 位运算工具类
 */
object BitUtil {
  /**
   * 获取指定偏移量的值
   * @param start 偏移量起始位置
   * @param end 偏移量结束位置
   * @param value 需要处理的值
   * @return 返回指定的值[Int]
   */
  @inline def getBitIntValue(start: Int, end: Int, value: Long): Int = {
    getBitLongValue(start, end, value).toInt
  }

  /**
   * 获取指定偏移量的值
   * @param start 偏移量起始位置
   * @param end 偏移量结束位置
   * @param value 需要处理的值
   * @return 返回指定的值[Long]
   */
  @inline def getBitLongValue(start: Int, end: Int, value: Long): Long = {
    (value >> start) & ((1 << (end - start + 1)) - 1)
  }

  /**
   * 设置指定偏移量的值
   * @param start 偏移量起始位置
   * @param end 偏移量结束位置
   * @param value 设置在指定偏移量的值
   * @param oldValue 老值
   * @return 指定偏移量被改变后的新值
   */
  @inline def setBitValue(start:Int,end:Int,value:Long,oldValue:Long):Long = {
    (oldValue & (~(((1 << (end - start + 1)) - 1) << start))) | (value << start )
  }

}
