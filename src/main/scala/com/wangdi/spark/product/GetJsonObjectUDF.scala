package com.wangdi.spark.product

import org.apache.spark.sql.api.java.UDF2
import com.alibaba.fastjson.JSON

/**
  * 技术点：自定义UDF函数
  * @author : wangdi
  * @time : creat in 2018/11/19 4:42 PM
  */
@SerialVersionUID(1L)
object GetJsonObjectUDF extends UDF2[String, String, String]{

  override def call(json: String, field: String): String = {
    val jsonObject = JSON.parseObject(json)
    jsonObject.getString(field)
  }
}
