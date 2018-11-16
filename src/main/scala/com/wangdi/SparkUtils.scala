package com.wangdi

import com.alibaba.fastjson.JSONObject
import com.wangdi.util.{Constants, ParamUtils}
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

/**
  * @ Author ：wang di
  * @ Date   ：Created in 1:12 PM 2018/11/1
  */
object SparkUtils {

  def getActionRDDByDateRange(sqlContext: SQLContext,taskParam:JSONObject):JavaRDD[Row]={
    val startTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from " + Constants.TABLE_USER_VISIT_ACTION + " where date>= " + startTime + " and date<= " + endTime
    val result =sqlContext.sql(sql)
    result.toJavaRDD
  }

}
