package com.blue.constant

import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp

object Constant extends Serializable {
  @transient
  val TABLE_NAME = "u_analysis:purchase_send_coupon_new6_hbase"
  @transient
  val FAMILY = "cf"
  @transient
  val QUALIFIER = "is_push"
  @transient
  val COMPARE_OP = CompareOp.EQUAL
  @transient
  val VALUE = "0"
}
