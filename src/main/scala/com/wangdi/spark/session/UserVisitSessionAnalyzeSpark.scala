package com.wangdi.spark.session

import java.util
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.wangdi.SparkUtils
import com.wangdi.conf.ConfigurationManager
import com.wangdi.dao.DaoFactory
import com.wangdi.model._
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
    if (task == null) {
      println(new Date() + ": connot find this task")
      return
    }
    val taskParam = JSON.parseObject(task.getTaskParam)

    val actionRDD = SparkUtils.getActionRDDByDateRange(sqlContext, taskParam)
    val sessionToActionRDD = actionRDD.rdd.mapPartitions(getSessionForKey)
    sessionToActionRDD.persist(StorageLevel.MEMORY_ONLY)
    //聚合成（sessionId，行为信息+用户信息）
    val userAggRDD = aggregateBySession(conf, sqlContext, sessionToActionRDD)
    //spark中累加器accumulator的使用
    val sessionAggrStatAccumulator = sc.accumulator(new String)(SessionAggrStatAccumulator)
    //按照task的要求进行过滤RDD[(String,String)]
    val filteredSessionidToAggrInfoRDD = filterSessionAndAggrStat(userAggRDD, taskParam, sessionAggrStatAccumulator)
    val sessionidToDetailRDD = getSessionIdToDetailRDD(filteredSessionidToAggrInfoRDD, sessionToActionRDD)
    //随机获取session
    randomExtractSession(sc,task.getTaskid,filteredSessionidToAggrInfoRDD,sessionidToDetailRDD)
    //计算出各个范围的session占比
    calculateAndPersistAggrStat(sessionAggrStatAccumulator.value,task.getTaskid)
    //获取top10热门品类
    val top10CategoryList = getTop10Category(task.getTaskid,sessionidToDetailRDD)
    //获取top10活跃session
    getTop10Session(sc,task.getTaskid,top10CategoryList,sessionidToDetailRDD)
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

  private def getSessionForKey[T](iterator: Iterator[Row]): Iterator[(String, Row)] = {
    var res = List[(String, Row)]()
    while (iterator.hasNext) {
      val cur = iterator.next()
      res ::= (cur.getString(2), cur)
    }
    res.iterator
  }


  private def aggregateBySession(sparkConf: SparkConf, sqlContext: SQLContext, dataList: RDD[(String, Row)]): RDD[(String, String)] = {
    val sessionToActionRDD = dataList.groupByKey()
    //聚合成（userid，行为信息）样子
    val useridToPartAggrInfoRDD = sessionToActionRDD.map { x =>
      val sessionid = x._1
      val iterator = x._2
      val searchKeywordsBuffer = new StringBuffer("")
      val clickCategoryIdsBuffer = new StringBuffer("")

      var userid: Option[Long] = None

      // session的起始和结束时间
      var startTime: Option[Date] = None
      var endTime: Option[Date] = None
      // session的访问步长
      var stepLength = 0

      // 遍历session所有的访问行为
      for (row <- iterator) {
        if (userid.isEmpty) {
          userid = Some(row.getLong(1))
        }
        val searchKeyword = row.getString(5)
        val clickCategoryId = row.getLong(6)
        if (StringUtils.isNotEmpty(searchKeyword)) if (!searchKeywordsBuffer.toString.contains(searchKeyword)) searchKeywordsBuffer.append(searchKeyword + ",")
        if (clickCategoryId != null) if (!clickCategoryIdsBuffer.toString.contains(String.valueOf(clickCategoryId))) clickCategoryIdsBuffer.append(clickCategoryId + ",")
        // 计算session开始和结束时间
        val actionTime = DateUtils.parseTime(row.getString(4))
        if (startTime.isEmpty) {
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
    val useridToInfoRDD = userInfoRDD.rdd.map(x => (x.getLong(0), x))
    val useridToFullInfoRDD = useridToPartAggrInfoRDD.join(useridToInfoRDD)
    //聚合成（sessionId，行为信息+用户信息）
    val useridToFullAggrInfoRDD = useridToFullInfoRDD.map { x =>
      val partAggrInfo = x._2._1
      val userInfoRow = x._2._2
      val sessionId = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID)
      val age = userInfoRow.getInt(3)
      val professional = userInfoRow.getString(4)
      val city = userInfoRow.getString(5)
      val sex = userInfoRow.getString(6)
      val fullAggrInfo = partAggrInfo + "|" + Constants.FIELD_AGE + "=" + age + "|" + Constants.FIELD_PROFESSIONAL + "=" + professional + "|" + Constants.FIELD_CITY + "=" + city + "|" + Constants.FIELD_SEX + "=" + sex
      (sessionId, fullAggrInfo)
    }

    useridToFullAggrInfoRDD
  }

  private def filterSessionAndAggrStat(sessionidToAggrInfoRDD: RDD[(String, String)],
                                       taskParam: JSONObject,
                                       sessionAggrStatAccumulator: Accumulator[String]): RDD[(String, String)] = {
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
    val filteredSessionidToAggrInfoRDD = sessionidToAggrInfoRDD.filter { x =>
      val aggrInfo = x._2
      if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
        Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
        //按照年龄过滤
        false
      } else if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL,
        parameter, Constants.PARAM_PROFESSIONALS)) { //按照职业过滤
        false
      } else if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY,
        parameter, Constants.PARAM_CITIES)) {
        //按照城市过滤
        false
      } else if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX,
        parameter, Constants.PARAM_SEX)) {
        //按照性别过滤
        false
      } else if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS,
        parameter, Constants.PARAM_KEYWORDS)) {
        //按照关键词过滤
        false
      } else if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS,
        parameter, Constants.PARAM_CATEGORY_IDS)) {
        //按照点击品类id进行过滤
        false
      } else {
        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT)
        //计算出session的访问时长和访问步长的范围，并进行相应的累加
        val visitLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
        val setpLength = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong
        //TODO 检验这个方法是否正确
        calculateStepLength(visitLength, sessionAggrStatAccumulator)
        calculateVisitLength(setpLength, sessionAggrStatAccumulator)
        true
      }
    }
    filteredSessionidToAggrInfoRDD
  }

  private def calculateVisitLength(visitLength: Long, sessionAggrStatAccumulator: Accumulator[String]): Unit = {
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
  private def calculateStepLength(stepLength: Long, sessionAggrStatAccumulator: Accumulator[String]): Unit = {
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
  private def getSessionIdToDetailRDD(sessionIdToAggrInfoRDD: RDD[(String, String)],
                                      sessionIdToActionRDD: RDD[(String, Row)]): RDD[(String, Row)] = {
    sessionIdToAggrInfoRDD.join(sessionIdToActionRDD).map(x => (x._2._1, x._2._2))
  }

  /**
    * 随机抽取session
    */
  private def randomExtractSession(sc: JavaSparkContext, taskid: Long, sessionidAggrInfoRDD: RDD[(String, String)],
                                   sessionidToActionRDD: RDD[(String, Row)]) = {
    //第一步，计算出每天每小时的session数量，获取// 获取<yyyy-MM-dd_HH,aggrInfo>格式的RDD
    val timeToSessionidRDD = sessionidAggrInfoRDD.mapPartitions { x => {
      var result = List[(String, String)]()
      while (x.hasNext) {
        val Rdd = x.next()
        val aggrInfo = Rdd._2
        val startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME)
        val dateHour = DateUtils.getDateHour(startTime)
        result ::= (dateHour, aggrInfo)
      }
      result.iterator
    }
    }
    val countMap = timeToSessionidRDD.countByKey()
    //第二步，使用按时间比例随机抽取算法，计算出每天每小时要抽取session的索引,
    //将<yyyy-MM-dd_HH,count>格式的map，转换成<yyyy-MM-dd,<HH,count>>的格式
    val dateHourCountMap = new mutable.HashMap[String, mutable.Map[String, Long]]()
    countMap.foreach { x =>
      val dateHour = x._1
      val date = dateHour.split("_")(0)
      val hour = dateHour.split("_")(1)
      val count = x._2
      val hourCountMap = dateHourCountMap.get(date)
      if (hourCountMap.isEmpty) {
        val tmpMap = new mutable.HashMap[String, Long]()
        tmpMap += (date -> count)
      } else {
        val tmpMap = hourCountMap.get
        tmpMap += (date -> count)
      }
    }

    /**
      * 实现按照比例随机抽取算法，总共要抽取100个session，先按照天数，进行平分
      * 用到了一个比较大的变量，随机抽取索引map
      * 之前是直接在算子里面使用了这个map，那么根据我们刚才讲的这个原理，每个task都会拷贝一份map副本
      * 还是比较消耗内存和网络传输性能的
      * 将map做成广播变量
      */
    val dateHourExtractMap = new mutable.HashMap[String, mutable.Map[String, List[Int]]]()
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
        val fastutilHourExtractMap = new mutable.HashMap[String, IntArrayList]()
        val fastutilExtractList = new IntArrayList()
        hourExtractMap.foreach { y =>
          val hour = y._1
          val extractList = y._2
          for (i <- extractList.indices) {
            fastutilExtractList.add(extractList(i))
          }
          fastutilHourExtractMap += (hour -> fastutilExtractList)
        }
        fastutilDateHourExtractMap += (date -> fastutilHourExtractMap)
      }

      //装成广播变量，调用sparkContext的broadcast方法传入参数即可
      val dateHourExtractMapBroadcast = sc.broadcast(fastutilDateHourExtractMap)

      //第三步：遍历每天每小时的session，然后根据随机索引进行抽取
      val timeToSessionRDD = timeToSessionidRDD.groupByKey()
      val extractSessionidsRDD = timeToSessionRDD.mapPartitions { x => {
        var extractSessionids = List[(String, String)]()
        while (x.hasNext) {
          val tmp = x.next()
          val dateHour = tmp._1
          val date = dateHour.split("_")(0)
          val hour = dateHour.split("_")(1)
          val iterator = tmp._2
          //使用广播变量的时候,直接调用广播变量（Broadcast类型）的value() / getValue()
          val dateHourExtractMap = dateHourExtractMapBroadcast.value
          val extractIndexList = dateHourExtractMap(date)(hour)
          val sessionRandomExtractDAO = DaoFactory.getSessionRandomExtractDAO
          var index = 0
          iterator.foreach { y =>
            val sessionAggrInfo = y
            if (extractIndexList.contains(index)) {
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
              extractSessionids ::= (sessionid, sessionid)
            }
            index += 1
          }
          extractSessionids
        }
        extractSessionids.iterator
      }}

      //第四步 获取抽取出来的session的明细数据
      val extractSessionDetailRDD = extractSessionidsRDD.join(sessionidToActionRDD)
      var sessionDetails = new util.ArrayList[SessionDetail]
      extractSessionDetailRDD.foreach{x=>
        val row = x._2._2
        val sessionDetail = new SessionDetail()
        sessionDetail.setTaskid(taskid)
        sessionDetail.setUserid(row.getLong(1))
        sessionDetail.setSessionid(row.getString(2))
        sessionDetail.setPageid(row.getLong(3))
        sessionDetail.setActionTime(row.getString(4))
        sessionDetail.setSearchKeyword(row.getString(5))
        sessionDetail.setClickCategoryId(row.getLong(6))
        sessionDetail.setClickProductId(row.getLong(7))
        sessionDetail.setOrderCategoryIds(row.getString(8))
        sessionDetail.setOrderProductIds(row.getString(9))
        sessionDetail.setPayCategoryIds(row.getString(10))
        sessionDetail.setPayProductIds(row.getString(11))

        sessionDetails.add(sessionDetail)
      }
      val sessionDetailDao = DaoFactory.getSessionDetailDAO
      sessionDetailDao.insertBatch(sessionDetails)

    }
  }

  /**
    * 计算各session范围占比，并写入Mysql
    * @param value
    * @param taskid
    */
  private def calculateAndPersistAggrStat(value:String,taskid:Long)={
      //从Accumulator统计串中获取值
    val session_count = StringUtils.getFieldFromConcatString(value,"\\|",Constants.SESSION_COUNT).toLong
    val visit_length_1s_3s = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1s_3s).toLong
    val visit_length_4s_6s = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_4s_6s).toLong
    val visit_length_7s_9s = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_7s_9s).toLong
    val visit_length_10s_30s = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10s_30s).toLong
    val visit_length_30s_60s = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30s_60s).toLong
    val visit_length_1m_3m = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1m_3m).toLong
    val visit_length_3m_10m = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_3m_10m).toLong
    val visit_length_10m_30m = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10m_30m).toLong
    val visit_length_30m = StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30m).toLong

    val step_length_1_3 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_1_3).toLong
    val step_length_4_6 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_4_6).toLong
    val step_length_7_9 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_7_9).toLong
    val step_length_10_30 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_10_30).toLong
    val step_length_30_60 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_30_60).toLong
    val step_length_60 = StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_60).toLong

    // 计算各个访问时长和访问步长的范围
    val visit_length_1s_3s_ratio = NumberUtils.formatDouble(visit_length_1s_3s.toDouble / session_count.toDouble, 2)
    val visit_length_4s_6s_ratio = NumberUtils.formatDouble(visit_length_4s_6s.toDouble / session_count.toDouble, 2)
    val visit_length_7s_9s_ratio = NumberUtils.formatDouble(visit_length_7s_9s.toDouble / session_count.toDouble, 2)
    val visit_length_10s_30s_ratio = NumberUtils.formatDouble(visit_length_10s_30s.toDouble / session_count.toDouble, 2)
    val visit_length_30s_60s_ratio = NumberUtils.formatDouble(visit_length_30s_60s.toDouble / session_count.toDouble, 2)
    val visit_length_1m_3m_ratio = NumberUtils.formatDouble(visit_length_1m_3m.toDouble / session_count.toDouble, 2)
    val visit_length_3m_10m_ratio = NumberUtils.formatDouble(visit_length_3m_10m.toDouble / session_count.toDouble, 2)
    val visit_length_10m_30m_ratio = NumberUtils.formatDouble(visit_length_10m_30m.toDouble / session_count.toDouble, 2)
    val visit_length_30m_ratio = NumberUtils.formatDouble(visit_length_30m.toDouble / session_count.toDouble, 2)

    val step_length_1_3_ratio = NumberUtils.formatDouble(step_length_1_3.toDouble / session_count.toDouble, 2)
    val step_length_4_6_ratio = NumberUtils.formatDouble(step_length_4_6.toDouble / session_count.toDouble, 2)
    val step_length_7_9_ratio = NumberUtils.formatDouble(step_length_7_9.toDouble / session_count.toDouble, 2)
    val step_length_10_30_ratio = NumberUtils.formatDouble(step_length_10_30.toDouble / session_count.toDouble, 2)
    val step_length_30_60_ratio = NumberUtils.formatDouble(step_length_30_60.toDouble / session_count.toDouble, 2)
    val step_length_60_ratio = NumberUtils.formatDouble(step_length_60.toDouble / session_count.toDouble, 2)

    // 将统计结果封装为Domain对象
    val sessionAggrStat = new SessionAggrStat
    sessionAggrStat.setTaskid(taskid)
    sessionAggrStat.setSession_count(session_count)
    sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio)
    sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio)
    sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio)
    sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio)
    sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio)
    sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio)
    sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio)
    sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio)
    sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio)
    sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio)
    sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio)
    sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio)
    sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio)
    sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio)
    sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio)

    // 调用对应的DAO插入统计结果
    val sessionAggrStatDAO = DaoFactory.getSessionAggrStatDAO
    sessionAggrStatDAO.insert(sessionAggrStat)
  }

  /**
    *获取top10热门品类
    */
  private def getTop10Category(taskid:Long,sessionidToDetailRDD:RDD[(String,Row)]):Array[(CategorySortKey,String)]={
    //第一步，获取符合条件的session访问过的所有品类
    val category = sessionidToDetailRDD.mapPartitions{x=>{
      var result = List[(Long,Long)]()
      while(x.hasNext){
        val tuple = x.next()
        val row = tuple._2
        val clickCategoryId = row.getLong(6)
        if(clickCategoryId != null){
          result::=(clickCategoryId,clickCategoryId)
        }

        val orderCategoryIds = row.getString(8)
        if(orderCategoryIds != null){
          val orderCategoryIdsSplited = orderCategoryIds.split(",")
          for(i <- 0 until orderCategoryIdsSplited.length){
            val ids = orderCategoryIdsSplited(i)
            result::=(ids.toLong,ids.toLong)
          }
        }

        val payCategoryIds = row.getString(8)
        if(payCategoryIds!= null){
          val payCategoryIdsSplited = payCategoryIds.split(",")
          for(i <- 0 until payCategoryIdsSplited.length){
            val ids = payCategoryIdsSplited(i)
            result::=(ids.toLong,ids.toLong)
          }
        }
      }

      result.iterator
    }}.distinct()

    //第二步，计算各品类的点击、下单、支付次数
    val clickCategoryIdToCountRDD = getClickCategoryIdToCountRDD(sessionidToDetailRDD)
    val orderCategoryIdToCountRDD = getOrderCategoryIdToCountRDD(sessionidToDetailRDD)
    val payCategoryIdToCountRDD = getPayCategoryIdToCountRDD(sessionidToDetailRDD)

    //第三步，join各品类与它的点击、下单和支付的次数
    val categoryidToCountRDD = joinCategoryAndData(category,clickCategoryIdToCountRDD,
      orderCategoryIdToCountRDD,payCategoryIdToCountRDD)

    //第四步，自定义二次排序key
    //第五步，将数据映射成<CategorySortKey,info>格式
    val sortKeyToCountRDD = categoryidToCountRDD.map{x=>
      val countInfo = x._2
      val clickCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong
      val sortKey = CategorySortKey(clickCount,orderCount,payCount)
      (sortKey,countInfo)
    }

    //第六步，用take（10）取出top10热门品类，写入MySql
    val top10CategoryDao = DaoFactory.getTop10CategoryDAO
    val top10CategoryList = sortKeyToCountRDD.take(10)
    for(i<- top10CategoryList.indices){
      val x = top10CategoryList(i)
      val countInfo = x._2
      val categoryId = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CATEGORY_ID).toLong
      val clickCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT).toLong
      val orderCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT).toLong
      val payCount = StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT).toLong

      val category = new TopTenCategory()
      category.setCategoryid(categoryId)
      category.setClickCount(clickCount)
      category.setOrderCount(orderCount)
      category.setPayCount(payCount)
      top10CategoryDao.insert(category)
    }

    top10CategoryList
  }

  /**
    *获取各品类点击RDD
    */
  private def getClickCategoryIdToCountRDD(sessionidTodetailRDD:RDD[(String,Row)]):RDD[(Long,Long)]={
    val clickActionRDD = sessionidTodetailRDD.filter{x=>
      val row = x._2
      if(row.get(6) != null){
        true
      }else{
        false
      }
    }

    val clickCategoryIdRDD = clickActionRDD.mapPartitions{x=>{
      var result = List[(Long,Long)]()
      while (x.hasNext){
        val tmp = x.next()
        val categoryId = tmp._2.getLong(6)
        result::=(categoryId,1L)
      }
      result.iterator
    }}

    val clickCategoryIdGroupRDD = clickCategoryIdRDD.groupByKey()
    clickCategoryIdGroupRDD.map(x=>(x._1,x._2.size.toLong))
  }

  /**
    *获取各品类下单RDD
    */
  private def getOrderCategoryIdToCountRDD(sessionidTodetailRDD:RDD[(String,Row)]):RDD[(Long,Long)]={
    val orderActionRDD = sessionidTodetailRDD.filter{x=>
      val row = x._2
      if(row.get(8) != null){
        true
      }else{
        false
      }
    }
    val orderCategoryIdRDD = orderActionRDD.mapPartitions{x=>{
      var result = List[(Long,Long)]()
      while(x.hasNext){
        val tmp = x.next()
        val row = tmp._2
        val orderCategoryIds = row.getString(8)
        val orderCategoryIdsSplited = orderCategoryIds.split(",")
        for(i <- 0 until orderCategoryIdsSplited.length){
          result::= (orderCategoryIdsSplited(i).toLong,1L)
        }
      }
      result.iterator
    }}
    val orderCategoryIdToGroupRDD = orderCategoryIdRDD.groupByKey()
    orderCategoryIdToGroupRDD.map(x=>(x._1,x._2.size.toLong))
  }

  /**
    * 获取各个品类的支付次数RDD
    */
  private def getPayCategoryIdToCountRDD(sessionidTodetailRDD:RDD[(String,Row)]):RDD[(Long,Long)]={
    val payActionRDD = sessionidTodetailRDD.filter{x=>
      val row = x._2
      if(row.get(10) != null){
        true
      }else{
        false
      }
    }
    val payCategoryIdRDD = payActionRDD.mapPartitions{x=>{
      var result = List[(Long,Long)]()
      while(x.hasNext){
        val tmp = x.next()
        val row = tmp._2
        val orderCategoryIds = row.getString(10)
        val orderCategoryIdsSplited = orderCategoryIds.split(",")
        for(i <- 0 until orderCategoryIdsSplited.length){
          result::= (orderCategoryIdsSplited(i).toLong,1L)
        }
      }
      result.iterator
    }}
    val payCategoryIdGroupRDD = payCategoryIdRDD.groupByKey()
    payCategoryIdGroupRDD.map(x=>(x._1,x._2.size.toLong))
  }

  /**
    * 连接品类RDD与数据RDD
    */
  private def joinCategoryAndData(categoryidRDD:RDD[(Long,Long)],
                                  clickCategoryIdTpCountRDD:RDD[(Long,Long)],
                                  orderCategoryIdToCountRDD:RDD[(Long,Long)],
                                  payCategoryIdToCountRDD:RDD[(Long,Long)]):RDD[(Long,String)]={
    val tmpJoinRDD = categoryidRDD.leftOuterJoin(clickCategoryIdTpCountRDD)
    var tmpMapRDD = tmpJoinRDD.mapPartitions{x=>{
      var result = List[(Long,String)]()
      while(x.hasNext){
        val tmp = x.next()
        val categoryId = tmp._1
        val optional = tmp._2._2
        val clickCount = if(optional.isEmpty){
          0L
        }else{
          optional.get
        }
        val value = Constants.FIELD_CATEGORY_ID + "=" + categoryId +
          "|" + Constants.FIELD_CLICK_COUNT + "=" + clickCount
        result::=(categoryId,value)
      }
      result.iterator
    }}

    tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryIdToCountRDD).mapPartitions{x=>{
      var result = List[(Long,String)]()
      while(x.hasNext){
        val tmp = x.next()
        val categoryId = tmp._1
        val optional = tmp._2._2
        val orderCount = if(optional.isEmpty){
          0L
        }else{
          optional.get
        }
        val value = tmp._2._1 + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount
        result::=(categoryId,value)
      }
      result.iterator
    }}

    tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryIdToCountRDD).mapPartitions{x=>{
      var result = List[(Long,String)]()
      while(x.hasNext){
        val tmp = x.next()
        val categoryId = tmp._1
        val optional = tmp._2._2
        val payCount = if(optional.isEmpty){
          0L
        }else{
          optional.get
        }
        val value = tmp._2._1+ "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        result::=(categoryId,value)
      }
      result.iterator
    }}

    tmpMapRDD
  }

  /**
    * 获取top10活跃session
    */
  private def getTop10Session(sc:JavaSparkContext,taskid:Long,
                              top10CategoryList:Array[(CategorySortKey,String)],sessionIdToDetailRDD:RDD[(String,Row)])={

    //第一步，将top10热门品类的id，生成一份RDD
    val top10CategoryIdList = top10CategoryList.map{x=>
      val categoryId = StringUtils.getFieldFromConcatString(x._2,"\\|",Constants.FIELD_CATEGORY_ID).toLong
      (categoryId,categoryId)
    }
    val top10CategoryIdRDD = sc.parallelize(top10CategoryIdList.toSeq)

    //第二步，计算top1-品类被各session点击次数
    val sessionIdToDetailsRDD = sessionIdToDetailRDD.groupByKey()
    val categoryidTosessionCountRDD = sessionIdToDetailsRDD.mapPartitions{x=>{
      var result = List[(Long,String)]()
      while (x.hasNext){
        val tmp = x.next()
        val sessionId = tmp._1
        val iterator = tmp._2
        val categoryCountMap = new mutable.HashMap[Long,Long]()
        for(row <-iterator){
          if(row.get(6)!=null){
            val categoryId = row.getLong(6)
            val count = categoryCountMap.getOrElse(categoryId,0L)
            val newCount = count+1
            categoryCountMap+=(categoryId->newCount)
          }
        }
        categoryCountMap.foreach{x=>
          val value = sessionId+","+x._2
          result::=(x._1,value)
        }
      }
      result.iterator
    }}

    //获得到top10热门品类，被各个session点击的次数
    val top10CategorySessionCountRDD = top10CategoryIdRDD.
      join(categoryidTosessionCountRDD).map(x=>(x._1,x._2._2))

    //第三步，分组取TopN算法实现，获取每个品类的top10活跃用户
    val top10CategorySessionCountsRDD = top10CategorySessionCountRDD.groupByKey()
    val top10SessionRDD = top10CategorySessionCountsRDD.mapPartitions{x=>{
      var result = List[(String,String)]()
      while(x.hasNext){
        val tmp = x.next()
        val categoryid = tmp._1
        val iterator = tmp._2.iterator

        //定义去topn的排序数组
        var top10Session = List[String]()
        while(iterator.hasNext){
          val sessionCount = iterator.next()
          val count = sessionCount.split(",")(1).toLong

          for (i <- top10Session.indices){
            if(top10Session(i) == null){
              top10Session(i) = sessionCount
            }else{
              val _count = top10Session(i).split(",")(1).toLong
              if(count > _count){
                for(j <- 9 to i){
                  top10Session(j) = top10Session(j-1)
                }
                top10Session(i) = sessionCount
              }
            }
          }
        }

        //将数据写入MySql中
        for(sessionCount <- top10Session){
          if(sessionCount != null){
            val sessionid = sessionCount.split(",")(0)
            val count = sessionCount.split(",")(1).toLong
            val top10Session = new TopTenSession
            top10Session.setTaskid(taskid)
            top10Session.setCategoryid(categoryid)
            top10Session.setSessionid(sessionid)
            top10Session.setClickCount(count)

            val top10SessionDAO = DaoFactory.getTop10SessionDAO
            top10SessionDAO.insert(top10Session)

            result::=(sessionid,sessionid)
          }
        }
      }
      result.iterator
    }}

    //第四步，获取top10活跃session的明细数据，并写入MySql
    val sessionDetailRDD = top10SessionRDD.join(sessionIdToDetailRDD)
    sessionDetailRDD.foreach{x=>
      val row = x._2._2
      val sessionDetail = new SessionDetail
      sessionDetail.setTaskid(taskid)
      sessionDetail.setUserid(row.getLong(1))
      sessionDetail.setSessionid(row.getString(2))
      sessionDetail.setPageid(row.getLong(3))
      sessionDetail.setActionTime(row.getString(4))
      sessionDetail.setSearchKeyword(row.getString(5))
      sessionDetail.setClickCategoryId(row.getLong(6))
      sessionDetail.setClickProductId(row.getLong(7))
      sessionDetail.setOrderCategoryIds(row.getString(8))
      sessionDetail.setOrderProductIds(row.getString(9))
      sessionDetail.setPayCategoryIds(row.getString(10))
      sessionDetail.setPayProductIds(row.getString(11))

      val sessionDetailDAO = DaoFactory.getSessionDetailDAO
      sessionDetailDAO.insert(sessionDetail)
    }
  }

}
