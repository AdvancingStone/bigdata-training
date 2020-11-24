package com.bluehonour.utils

object HtmlUtil {

  def getTableHtml(tableHead: List[Any], tableBody: List[List[Any]]): String = {
    val html = new StringBuilder("<table border=\"1\"")
    html.append("<thead>").append("<tr>")
    tableHead.foreach(th => html.append("<td>").append(th).append("</td>"))
    html.append("</tr>").append("</thead>").append("<tbody>")
    tableBody.foreach(tbs => {
      html.append("<tr>")
      tbs.foreach(tb => html.append("<td>").append(tb).append("</td>"))
      html.append("</tr>")
    })
    html.append("</tbody></table>")
    html.toString
  }
}
