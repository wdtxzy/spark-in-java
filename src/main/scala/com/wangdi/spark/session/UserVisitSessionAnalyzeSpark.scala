package com.wangdi.spark.session

import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangdi.SparkUtils
import com.wangdi.conf.ConfigurationManager
import com.wangdi.dao.DaoFactory
import com.wangdi.model.SessionRandomExtract
import com.wangdi.test.MockData
import com.wangdi.util._
import org.apache.parquet.it.unimi.dsi.fastutil.ints.{IntArrayList, IntList}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

import scala.collection.mutable
import scala.util.Random

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
    val taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION)
    val task = taskDao.findById(taskid)
    if(task == null){
      println(new Date() +": connot find this task")
      return
    }
    val taskParam = JSON.parseObject(task.getTaskParam)

    val actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext,taskParam)
    val sessionToActionRDD = actionRDD.rdd.mapPartitions(getSessionForKey)
    sessionToActionRDD.persist(StorageLevel.MEMORY_ONLY)
    //聚合成（sessionId，行为信息+用户信息）
    val userAggRDD = aggregateBySession(conf,sqlContext,sessionToActionRDD)
    val sessionAggrStatAccumulator = sc.accumulator("", SessionAggrStatAccumulator())
    //按照task的要求进行过滤RDD[(String,String)]
    val filteredSessionidToAggrInfoRDD = filterSessionAndAggrStat(userAggRDD,taskParam,sessionAggrStatAccumulator)
    val sessionidToDetailRDD = getSessionIdToDetailRDD(filteredSessionidToAggrInfoRDD,sessionToActionRDD)

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

  private def  getSessionForKey[T](iterator: Iterator[Row]):Iterator[(String,Row)]={
    var res = List[(String,Row)]()
    while (iterator.hasNext){
      val cur = iterator.next()
      res::=(cur.getString(2),cur)
    }
    res.iterator
  }


  private def aggregateBySession(sparkConf: SparkConf,sqlContext:SQLContext,dataList: RDD[(String,Row)]):RDD[(String,String)]={
      val sessionToActionRDD = dataList.groupByKey()
    //聚合成（userid，行为信息）样子
      val useridToPartAggrInfoRDD = sessionToActionRDD.map{x=>
        val sessionid = x._1
        val iterator = x._2
        val searchKeywordsBuffer = new StringBuffer("")
        val clickCategoryIdsBuffer = new StringBuffer("")

        var userid:Option[Long] = None

        // session的起始和结束时间
        var startTime:Option[Date] = None
        var endTime:Option[Date] = None
        // session的访问步长
        var stepLength = 0

        // 遍历session所有的访问行为
        for(row <- iterator) {
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
    val useridToInfoRDD = userInfoRDD.rdd.map(x =>(x.getLong(0),x))
    val useridToFullInfoRDD = useridToPartAggrInfoRDD.join(useridToInfoRDD)
    //聚合成（sessionId，行为信息+用户信息）
    val useridToFullAggrInfoRDD = useridToFullInfoRDD.map{x=>
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

  private def filterSessionAndAggrStat(sessionidToAggrInfoRDD: RDD[(String, String)],
                                       taskParam: JSONObject,
                                       sessionAggrStatAccumulator: Accumulator[String]):RDD[(String,String)]={
    val startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var _parameter = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|"
    else "") + (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|"
    else "") + (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|"
    else "") + (if (cities != null) Constants.PARAM_CITIES + "=" + cities + "|"
    else "") + (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|"
    else "") + (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|"
    else "") + (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds
    else "")

    if (_parameter.endsWith("\\|")) _parameter = _parameter.substring(0, _parameter.length - 1)

    val parameter = _parameter
    val filteredSessionidToAggrInfoRDD = sessionidToAggrInfoRDD.filter{x=>
      val aggrInfo = x._2
      if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,
        Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){//按照年龄过滤
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,
        parameter,Constants.PARAM_PROFESSIONALS)){ //按照职业过滤
        false
      }else if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,
        parameter,Constants.PARAM_CITIES)){//按照城市过滤
        false
      }else if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,
        parameter,Constants.PARAM_SEX)){//按照性别过滤
        false
      }else if(!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
        parameter, Constants.PARAM_KEYWORDS)){//按照关键词过滤
        false
      }else if(!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
        parameter, Constants.PARAM_CATEGORY_IDS)){//按照点击品类id进行过滤
        false
      }else{
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
        //计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH).toLong
        val setpLength = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH).toLong
        //TODO 检验这个方法是否正确
        calculateStepLength(visitLength,sessionAggrStatAccumulator)
        calculateVisitLength(setpLength,sessionAggrStatAccumulator)
        true
      }
    }
    filteredSessionidToAggrInfoRDD
  }

  private def calculateVisitLength(visitLength: Long,sessionAggrStatAccumulator: Accumulator[String]): Unit={
    if (visitLength >= 1 && visitLength <= 3) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength >= 4 && visitLength <= 6) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength >= 7 && visitLength <= 9) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength >= 10 && visitLength <= 30) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength > 30 && visitLength <= 60) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800) sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m)
  }

  /**
    * 计算访问步长范围
    *
    * @param stepLength
    */
  private def calculateStepLength(stepLength: Long,sessionAggrStatAccumulator: Accumulator[String]): Unit={
    if (stepLength >= 1 && stepLength <= 3) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength >= 4 && stepLength <= 6) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength >= 7 && stepLength <= 9) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength >= 10 && stepLength <= 30) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength > 30 && stepLength <= 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60) sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60)
  }

  /**
    * 获取通过筛选条件的session的访问明细数据RDD
    */
  private def getSessionIdToDetailRDD(sessionIdToAggrInfoRDD:RDD[(String,String)],
                                      sessionIdToActionRDD:RDD[(String,Row)]):RDD[(String,Row)]={
     sessionIdToAggrInfoRDD.join(sessionIdToActionRDD).map(x=>(x._2._1,x._2._2))
  }

  /**
    * 随机抽取session
    */
  private def randomExtractSession(sc:JavaSparkContext,taskid:Long,sessionidAggrInfoRDD:RDD[(String,String)],
                                   sessionidToActionRDD:RDD[(String,Row)])={
    //第一步，计算出每天每小时的session数量，获取// 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val timeToSessionidRDD = sessionidAggrInfoRDD.mapPartitions{x=>{
      var result = List[(String,String)]()
      while(x.hasNext){
        val Rdd = x.next()
        val aggrInfo = Rdd._2
        val startTime = StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(startTime)
        result::=(dateHour,aggrInfo)
      }
      result.iterator
    }}
    val countMap = timeToSessionidRDD.countByKey()
    //第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引,
    //将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = new mutable.HashMap[String,mutable.Map[String,Long]]()
    countMap.foreach{x=>
      val dateHour = x._1
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      val count = x._2
      val hourCountMap = dateHourCountMap.get(date)
      if(hourCountMap.isEmpty){
        val tmpMap = new mutable.HashMap[String,Long]()
        tmpMap+=(date ->count)
      }else{
        val tmpMap=hourCountMap.get
        tmpMap+=(date ->count)
      }
    }

    /**
      * 实现按照比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
      * 用到了一个比较大的变量，随机抽取索引map
      * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
      * 还是比较消耗内存和网络传输性能的
      * 将map做成广播变量
      */
    val dateHourExtractMap = new mutable.HashMap[String,mutable.Map[String,List[Int]]]()
    val random = new Random()
    dateHourCountMap.foreach { x =>
      val extraNumberPerDay = 100 / dateHourCountMap.size
      val date = x._1
      val hourCountMap = x._2
      var sessionCount = 0L
      hourCountMap.foreach(x => sessionCount += x._2)
      var hourExtractMap = dateHourExtractMap(date)

      if (hourCountMap.isEmpty) {
        hourExtractMap = new mutable.HashMap[String, List[Int]]()
        dateHourExtractMap += (date -> hourExtractMap)
      }

      hourCountMap.foreach { x =>
        val hour = x._1
        val count = x._2
        var hourExtractNumber = ((count / sessionCount) * extraNumberPerDay).toInt
        if (hourExtractNumber > count) {
          hourExtractNumber = count.toInt
        }

        // 先获取当前小时的存放随机数的list
        var extractIndexList = hourExtractMap(hour)
        if (extractIndexList.isEmpty) {
          extractIndexList = List[Int]()
          hourExtractMap.put(hour, extractIndexList)
        }

        // 生成上面计算出来的数量的随机数
        for (_ <- 0 to hourExtractNumber) {
          var extractIndex = random.nextInt(count.toInt)
          while (extractIndexList.contains(extractIndex)) {
            extractIndex = random.nextInt(count.toInt)
          }
          extractIndexList ::= extractIndex
        }
      }


      val fastutilDateHourExtractMap = new mutable.HashMap[String, mutable.Map[String, IntArrayList]]()
      dateHourExtractMap.foreach { x =>
        val date = x._1
        val hourExtractMap = x._2
        val fastutilHourExtractMap = new mutable.HashMap[String,IntArrayList]()
        val fastutilExtractList = new IntArrayList()
        hourExtractMap.foreach{y =>
          val hour = y._1
          val extractList = y._2
          for (i <- extractList.indices) {
            fastutilExtractList.add(extractList(i))
          }
          fastutilHourExtractMap+=(hour->fastutilExtractList)
        }
        fastutilDateHourExtractMap+=(date->fastutilHourExtractMap)
      }

      //装成广播变量，调用sparkContext的broadcast方法传入参数即可
      val dateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap)

      //第三步：遍历每天每小时的session，然后根据随机索引进行抽取
      val timeToSessionRDD = timeToSessionidRDD.groupByKey()
      val extractSessionidsRDD = timeToSessionRDD.map{x=>
        var extractSessionids = List[(String,String)]()
        val dateHour = x._1
        val date = dateHour.split("_")(0)
        val hour = dateHour.split("_")(1)
        val iterator = x._2
        //使用广播变量的时候,直接调用广播变量（Broadcast类型）的value() / getValue()
        val dateHourExtractMap = dateHourExtractMapBroadcast.value
        val extractIndexList = dateHourExtractMap(date)(hour)
        val sessionRandomExtractDAO = DaoFactory.getSessionRandomExtractDAO
        var index = 0
        iterator.foreach{y=>
          val sessionAggrInfo = y
          if(extractIndexList.contains(index)){
            val sessionid = StringUtils.getFieldFromConcatString(sessionAggrInfo,
              "\\|", Constants.FIELD_SESSION_ID)
            val sessionRandomExtract = new SessionRandomExtract()
            sessionRandomExtract.setTaskid(taskid)
            sessionRandomExtract.setSessionid(sessionid)
            sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_START_TIME))
            sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS))
            sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(
              sessionAggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS))
            sessionRandomExtractDAO.insert(sessionRandomExtract)
            extractSessionids::=(sessionid,sessionid)
          }
          index+=1
        }
        extractSessionids
      }

      //第四步
    }
  }
}
