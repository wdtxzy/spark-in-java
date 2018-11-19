package com.wangdi.spark.product

import org.apache.spark.sql.api.java.UDF1

/**
  * 去除随机前缀
  * @author : wangdi
  * @time : creat in 2018/11/19 4:53 PM
  */
object RemoveRandomPrefixUDF extends UDF1[String, String]{

  override def call(value: String): String = {
    val valueSplit = value.split("_")
    valueSplit(1)
  }
}
