package com.bluehonour.excel

import org.apache.poi.xssf.usermodel.{XSSFRow, XSSFSheet, XSSFWorkbook}

object Demo {
  def main(args: Array[String]): Unit = {


    val filePath: String = this.getClass.getClassLoader.getResource("a.xlsx").getPath
    println(filePath)
    val workbook: XSSFWorkbook = new XSSFWorkbook(filePath)

    for(i <- 0 until workbook.getNumberOfSheets){
      // 获取表格每一个sheet
      val xssfSheet: XSSFSheet = workbook.getSheetAt(i)
      val sheetName: String = xssfSheet.getSheetName
      println(sheetName)

      for(row <- 0 to xssfSheet.getLastRowNum){
        val xssfRow: XSSFRow = xssfSheet.getRow(row)
        for(column <- 0 until xssfRow.getPhysicalNumberOfCells){
          val value: String = xssfRow.getCell(column).toString
          println(value)
        }
      }

//      val titleRow = xssfSheet.getRow(0)
//      for(row <- 1 to xssfSheet.getLastRowNum){
//        //获取表格每一行
//        val xssfRow: XSSFRow = xssfSheet.getRow(0)
//        for(i <- 0 until xssfRow.getPhysicalNumberOfCells){
//          // 获取表格每一列
//          val title = titleRow.getCell(i).toString
//          val value = xssfRow.getCell(i).toString
//          print(title + ": " + value + " ")
//        }
//        println()
//      }

    }
  }
}
