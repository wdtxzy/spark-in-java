package com.wangdi.spark.product

import java.util

import com.alibaba.fastjson.JSON
import com.wangdi.SparkUtils
import com.wangdi.conf.ConfigurationManager
import com.wangdi.dao.DaoFactory
import com.wangdi.util.{Constants, ParamUtils}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, RowFactory, SQLContext}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
  * @author : wangdi
  * @time : creat in 2018/11/19 2:37 PM
  */
object AreaTop3ProductSpark {

  def main(args: Array[String]): Unit = {
    //创建sparkConf
    val conf = new SparkConf().setAppName("AreaTop3ProductSpark")
    SparkUtils.setMaster(conf)
    //构建spark上下文
    val sc = new JavaSparkContext(conf)
    val sqlContext =SparkUtils.getSQLContext(sc)

    // 注册自定义函数
    sqlContext.udf.register("concat_long_string", ConcatLongStringUDF, DataTypes.StringType)
    sqlContext.udf.register("get_json_object", GetJsonObjectUDF, DataTypes.StringType)
    sqlContext.udf.register("random_prefix", RandomPrefixUDF, DataTypes.StringType)
    sqlContext.udf.register("remove_random_prefix", RemoveRandomPrefixUDF, DataTypes.StringType)
    sqlContext.udf.register("group_concat_distinct", GroupConcatDistinctUDAF)

    //生成模拟数据
    SparkUtils.mockData(sc,sqlContext)
    val taskDao = DaoFactory.getTaskDAO

    val taskId = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT)
    val task = taskDao.findById(taskId)

    val taskParam = JSON.parseObject(task.getTaskName)
    val startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    //查询用户指定日期范围内的点击行为数据，Hive数据源的使用
    val cityidToclickActionRDD = getCityIdToClickActionRDDByDate(sqlContext,
      startDate,endDate)
    // 从MySQL中查询城市信息，技术点2：异构数据源之MySQL的使用
    val cityIdToCityInfoRDD = getCityidToCityInfoRDD(sqlContext)
    //生成点击商品基础信息临时表,将RDD转换为DataFrame，并注册临时表

  }

  /**
    * 查询指定日期范围内的点击行为数据
    * @param sqlContext
    * @param startDate
    * @param endDate
    * @return 点击行为数据
    */
  private def getCityIdToClickActionRDDByDate(sqlContext:SQLContext,
                                              startDate:String,endDate:String): RDD[(Long,Row)] ={
    // 从user_visit_action中，查询用户访问行为数据
    // 第一个限定：click_product_id，限定为不为空的访问行为，那么就代表着点击行为
    // 第二个限定：在用户指定的日期范围内的数据

    val sql = "SELECT " + "city_id," + "click_product_id product_id " +
      "FROM user_visit_action " + "WHERE click_product_id IS NOT NULL " +
      "AND date>='" + startDate + "' " + "AND date<='" + endDate + "'"

    val clickActionDF = sqlContext.sql(sql)
    val clickActionRDD = clickActionDF.rdd
    clickActionRDD.map{x=>
      val cityid = x.getLong(0)
      (cityid,x)
    }
  }

  /**
    * 使用Spark SQL从MySQL中查询城市信息
    * @param sqlContext
    * @return
    */
  private def getCityidToCityInfoRDD(sqlContext:SQLContext):RDD[(Long,Row)]= {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    val url = if(local) ConfigurationManager.getProperty(Constants.JDBC_URL) else ConfigurationManager.getProperty(Constants.JDBC_URL_PROD)
    val user = if(local) ConfigurationManager.getProperty(Constants.JDBC_USER) else ConfigurationManager.getProperty(Constants.JDBC_USER_PROD)
    val password = if(local) ConfigurationManager.getProperty(Constants.JDBC_PASSWORD) else ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD)

    val options = new mutable.HashMap[String,String]()
    options +=("url"->url)
    options +=("dbtable"->"city_info")
    options +=("user"->user)
    options +=("password"->password)

    val cityInfoDF = sqlContext.read.format("jdbc").options(options).load()
    val cityInfoRDD = cityInfoDF.rdd
    cityInfoRDD.map{x=>
      val cityId = x.get(0).toString.toLong
      (cityId,x)
    }
  }

  private def generateTempClickProductBasicTable(sqlContext:SQLContext,
                                                 cityIdToClickActionRDD:RDD[(Long,Row)],
                                                 cityIdToCityInfoRDD:RDD[(Long,Row)])={
    val joinRDD = cityIdToClickActionRDD.join(cityIdToCityInfoRDD)
    val mapRDD= joinRDD.map{x=>
      val cityid = x._1
      val clickAction = x._2._1
      val cityInfo = x._2._2

      val productId = clickAction.getLong(1)
      val cityName = cityInfo.getString(1)
      val area = cityInfo.getString(2)

      RowFactory.create(cityid,cityName,area,productId)
    }
    val structFields = new util.ArrayList[StructField]()
    structFields.add(DataTypes.createStructField("city_id", DataTypes.LongType, true))
    structFields.add(DataTypes.createStructField("city_name", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("area", DataTypes.StringType, true))
    structFields.add(DataTypes.createStructField("product_id", DataTypes.LongType, true))

    val schema = DataTypes.createStructType(structFields)
    val df = sqlContext.createDataFrame(mapRDD, schema)
    // 将DataFrame中的数据，注册成临时表（tmp_click_product_basic）
    df.createOrReplaceTempView("tmp_click_product_basic")
  }


}
