package com.wangdi.spark.product

import org.apache.spark.sql.api.java.UDF3

/**
  * 将两个字符串拼接起来
  * @author : wangdi
  * @time : creat in 2018/11/19 4:01 PM
  */
@SerialVersionUID(1L)
object ConcatLongStringUDF extends UDF3[Long, String, String, String]{

  override def call(t1: Long, t2: String, split: String): String = t1.toString+split+t2

}
