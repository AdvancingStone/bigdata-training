package com.blue.excel

import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFSheet, XSSFWorkbook}

object Test {
  def main(args: Array[String]): Unit = {
    val filePath: String = this.getClass.getClassLoader.getResource("test.xlsx").getPath
    println(filePath)
    val workbook: XSSFWorkbook = new XSSFWorkbook(filePath)
    val features: XSSFSheet = workbook.getSheet("Features")
    val featureName: String = features.getSheetName
    println(featureName)

    for(row <- 0 until features.getLastRowNum ){
      val sb = new StringBuffer
      val xssfRow: XSSFRow = features.getRow(row)
      if(xssfRow.getPhysicalNumberOfCells !=null){
        for(column <- 0 until xssfRow.getPhysicalNumberOfCells){
          if( xssfRow.getCell(column) != null){
            val value: String = xssfRow.getCell(column).toString
            //        println(value)
            sb.append(value+",")
          }

        }
        println(sb.toString.substring(0, sb.length()-1))
      }

    }

  }

}
