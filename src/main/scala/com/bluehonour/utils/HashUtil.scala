package com.bluehonour.utils

import java.nio.charset.Charset

import com.google.common.hash.{HashFunction, Hashing}

/**
 * hash工具类
 */
object HashUtil {

  private val mur128Func: HashFunction = Hashing.murmur3_128()

  private val md5Func: HashFunction = Hashing.md5()

  /**
   * string hash 转 long
   */
  def hashToLong(in: String): Long = mur128Func.hashString(in, Charset.defaultCharset()).padToLong()

  /**
   * long md5
   * @param in 需要转换的值
   * @return md5
   */
  def long2Md5(in:Long):String = md5Func.hashLong(in).toString

  /**
   * string To md5
   * @param in 需要转换的值
   * @return md5
   */
  def str2Md5(in:String):String = md5Func.hashString(in,Charset.defaultCharset()).toString

}
