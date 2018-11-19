package com.wangdi

import com.alibaba.fastjson.JSONObject
import com.wangdi.conf.ConfigurationManager
import com.wangdi.test.MockData
import com.wangdi.util.{Constants, ParamUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaRDD, JavaSparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

/**
  * @ Author ：wang di
  * @ Date   ：Created in 1:12 PM 2018/11/1
  */
object SparkUtils {

  /**
    * 获取指定日期范围内的用户行为数据RDD
    * @param sqlContext
    * @param taskParam
    * @return
    */
  def getActionRDDByDateRange(sqlContext: SQLContext,taskParam:JSONObject):JavaRDD[Row]={
    val startTime = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endTime = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql = "select * from " + Constants.TABLE_USER_VISIT_ACTION + " where date>= " + startTime + " and date<= " + endTime
    val result =sqlContext.sql(sql)
    result.toJavaRDD
  }

  /**
    * 根据当前是否本地测试的配置
    * @param conf
    */
  def setMaster(conf:SparkConf)={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if(local){
      conf.setMaster("local")
    }
  }

  /**
    * 获取sqlContext
    * @param sc
    * @return
    */
  def getSQLContext(sc:SparkContext):SQLContext={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) new SQLContext(sc)
    else new HiveContext(sc)
  }

  /**
    * 生成模拟数据
    * @param sc
    * @param sqlContext
    */
  def mockData(sc:JavaSparkContext,sqlContext: SQLContext): Unit ={
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) MockData.mock(sc, sqlContext)
  }

}
