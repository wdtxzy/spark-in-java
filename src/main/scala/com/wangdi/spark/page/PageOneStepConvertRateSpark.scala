package com.wangdi.spark.page

import util.control.Breaks._
import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangdi.SparkUtils
import com.wangdi.dao.DaoFactory
import com.wangdi.util.{Constants, DateUtils, ParamUtils}
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row


/**
  * 页面单条转化率模块spark作业
  *
  * @author : Wang D
  * @time : creat in 2018/11/20 10:46 AM
  */
object PageOneStepConvertRateSpark {

  def main(args: Array[String]): Unit = {
    //构建Spark上下文
    val conf = new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE)
    SparkUtils.setMaster(conf)
    val sc = new JavaSparkContext(conf)
    val sqlContext = SparkUtils.getSQLContext(sc.sc)
    //生成模拟数据
    SparkUtils.mockData(sc,sqlContext)
    //查询任务，获取任务参数
    val taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE)
    val taskDao = DaoFactory.getTaskDAO
    val task = taskDao.findById(taskid)
    if(task == null){
      return
    }

    val taskParam = JSON.parseObject(task.getTaskParam)
    //查询指定日期范围内的用户访问行为数据
    val actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,taskParam)
    //获取<sessionid,用户访问行为>的数据
    val sessionidToActionRDD = actionRDD.rdd.map{x=>(x.getString(2),x)}
    //要拿到每个session对应的访问行为数据，才能够去生成切片
    val sessionIdToActionRDD = sessionidToActionRDD.groupByKey()

  }

  /**
    * 页面切片生成及算法
    * @param sc
    * @param sessionIdToActionRDD
    * @param taskParam
    * @return
    */
  private def generateAndMatchPageSplit(sc:JavaSparkContext,sessionIdToActionRDD:RDD[(String,Iterable[Row])],
                                        taskParam:JSONObject):RDD[(String,Int)]={
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val targetPageFlowBroadcast = sc.broadcast(targetPageFlow)
    sessionIdToActionRDD.mapPartitions{x=>{
      var result = List[(String,Int)]()
      while(x.hasNext){
        val tmp = x.next()
        val iterator = tmp._2
        val targetPages = targetPageFlowBroadcast.value.split(",")
        var rows = List[Row]()
        iterator.foreach(x=>rows::=x)
        rows.sortWith{
          case (row1,row2)=>{DateUtils.parseTime(row1.getString(4)).getTime-
            DateUtils.parseTime(row2.getString(4)).getTime>0}
        }
        var lastPageId = 0L
        for(row<-rows){
          val pageid = row.getLong(3)
          breakable{
            if(lastPageId == 0L){
              lastPageId = pageid
              break()
            }
            val pageSplit = lastPageId + "_" +pageid
            for(i<- targetPages.indices){
              val targetPageSplit = targetPages(i-1)+"_"+targetPages(i)
              if(pageSplit.equals(targetPageSplit)){
                result::=(pageSplit,1)
                break()
              }
            }
            lastPageId = pageid
          }

        }
      }
      result.iterator
    }}
  }

  private def generateAndMatchPageSplit(sessionidToactionsRDD:RDD[(String,Iterable[Row])],
                                        taskParam:JSONObject):RDD[(String,Int)]={
    val targetPageFlow = ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW)
    val startPageId = targetPageFlow.split(",")(0).toLong

  }
}
