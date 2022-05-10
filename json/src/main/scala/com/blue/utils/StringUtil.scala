package com.blue.utils

object StringUtil {

  def combineString(preStr: String, suffixStr: String): String = {
    preStr + suffixStr.charAt(0).toUpper + suffixStr.substring(1)
  }
}
