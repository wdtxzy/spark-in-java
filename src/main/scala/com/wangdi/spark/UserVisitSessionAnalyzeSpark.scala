package com.wangdi.spark


import java.util
import java.util.{ArrayList, Date, Iterator, List}

import org.apache.spark.sql.functions.col
import com.wangdi.conf.ConfigurationManager
import com.wangdi.dao.DaoFactory
import com.wangdi.test.MockData
import com.wangdi.util.{Constants, DateUtils, ParamsUtils, StringUtils}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Dataset, Row, SQLContext}
import com.wangdi.SparkUtils
import com.alibaba.fastjson.JSON
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.storage.StorageLevel

/**
  * @ Author ：wang di
  * @ Date   ：Created in 1:16 PM 2018/11/1
  */
object UserVisitSessionAnalyzeSpark {

  def main(args: Array[String]): Unit = {
    //构建spark上下文
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION).setMaster("local")

    val sc = new JavaSparkContext(conf)
    val sqlContext = getSQLContext(sc.sc)

    //生成模拟数据
    mockData(sc, sqlContext)

    //创建DAO工厂
    val taskDao = DaoFactory.getTaskDAO

    //查询出指定任务
    val taskid = ParamsUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    val task = taskDao.findById(taskid)
    if(task == null){
      println(new Date() +": connot find this task")
      return
    }
    val taskParam = JSON.parseObject(task.getTaskParam)
    val actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,taskParam)
    val sessionToActionRDD:JavaPairRDD[String,Row] = actionRDD.mapToPair(x=>(x.getString(2),x))
    sessionToActionRDD.persist(StorageLevel.MEMORY_ONLY)
    //聚合成（sessionId，行为信息+用户信息）
    val userAggRDD = aggregateBySession(conf,sqlContext,sessionToActionRDD)


    //关闭spark上下文
    sc.close()
  }

  //获取SQLContext
  private def getSQLContext(sc: SparkContext) = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) new SQLContext(sc)
    else new HiveContext(sc)
  }

  //生成模拟数据（只有本地模式，才会去生成模拟数据）
  private def mockData(sc: JavaSparkContext, sqlContext: SQLContext): Unit = {
    val local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL)
    if (local) MockData.mock(sc, sqlContext)
  }

  def getSessionidToActionRDD(activeRDD: JavaRDD[Row]):JavaPairRDD[String,Row]=
    activeRDD.mapPartitionsToPair((iterator:util.Iterator[Row])=>{
     val list :List[(String,Row)] = new util.ArrayList[(String,Row)]
      while (iterator.hasNext){
        val row = iterator.next()
        list.add(new Tuple2[String,Row](row.getString(2),row))
      }
      list.iterator()
    })

  def aggregateBySession(sparkConf: SparkConf,sqlContext:SQLContext,javaPairRDD: JavaPairRDD[String,Row]):JavaPairRDD[String,String]={
      val sessionToActionRDD = javaPairRDD.groupByKey()
    //聚合成（userid，行为信息）样子
      val useridToPartAggrInfoRDD:JavaPairRDD[Long,String] = sessionToActionRDD.mapToPair{x=>
        val sessionid = x._1
        val iterator = x._2.iterator()
        val searchKeywordsBuffer = new StringBuffer("")
        val clickCategoryIdsBuffer = new StringBuffer("")

        var userid:Option[Long] = None

        // session的起始和结束时间
        var startTime:Option[Date] = None
        var endTime:Option[Date] = None
        // session的访问步长
        var stepLength = 0

        // 遍历session所有的访问行为
        while (iterator.hasNext) {
          val row = iterator.next
          if (userid.isEmpty) {
            userid = Some(row.getLong(1))
          }
          val searchKeyword = row.getString(5)
          val clickCategoryId = row.getLong(6)
          if (StringUtils.isNotEmpty(searchKeyword)) if (!searchKeywordsBuffer.toString.contains(searchKeyword)) searchKeywordsBuffer.append(searchKeyword + ",")
          if (clickCategoryId != null) if (!clickCategoryIdsBuffer.toString.contains(String.valueOf(clickCategoryId))) clickCategoryIdsBuffer.append(clickCategoryId + ",")
          // 计算session开始和结束时间
          val actionTime = DateUtils.parseTime(row.getString(4))
          if (startTime.isEmpty){
            startTime = Some(actionTime)
          }
          if (endTime.isEmpty) {
            endTime = Some(actionTime)
          }
          if (actionTime.before(startTime.get)) {
            startTime = Some(actionTime)
          }
          if (actionTime.after(endTime.get)) {
            endTime = Some(actionTime)
          }
          stepLength += 1
        }

        val searchKeywords = StringUtils.trimComma(searchKeywordsBuffer.toString)
        val clickCategoryIds = StringUtils.trimComma(clickCategoryIdsBuffer.toString)

        // 计算session访问时长（秒）
        val visitLength = (endTime.get.getTime - startTime.get.getTime) / 1000
        val partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords + "|" + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|" + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" + Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime.get)
        (userid.get, partAggrInfo)
      }

    val sql = "select * from user_info"
    val userInfoRDD = sqlContext.sql(sql).toJavaRDD
    val useridToInfoRDD:JavaPairRDD[Long,Row] = userInfoRDD.mapToPair(x =>(x.getLong(0),x))
    val useridToFullInfoRDD:JavaPairRDD[Long,(String, Row)] = useridToPartAggrInfoRDD.join(useridToInfoRDD)
    //聚合成（sessionId，行为信息+用户信息）
    val useridToFullAggrInfoRDD:JavaPairRDD[String,String] = useridToFullInfoRDD.mapToPair{x=>
      val partAggrInfo = x._2._1
      val userInfoRow = x._2._2
      val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID)
      val age = userInfoRow.getInt(3)
      val professional = userInfoRow.getString(4)
      val city = userInfoRow.getString(5)
      val sex = userInfoRow.getString(6)
      val fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex
      (sessionId,fullAggrInfo)
    }
    useridToFullAggrInfoRDD
  }


}
