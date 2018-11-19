package com.wangdi.spark.product

import org.apache.spark.sql.api.java.UDF2

import scala.util.Random

/**
  * random_prefix()
  * @author : wangdi
  * @time : creat in 2018/11/19 4:48 PM
  */
@SerialVersionUID(1L)
object RandomPrefixUDF extends UDF2[String, Integer, String]{

  override def call(value: String, num: Integer): String = {
    val random = new Random()
    val randNum = random.nextInt(10)
    randNum+"_"+value
  }
}
