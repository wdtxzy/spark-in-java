package com.wangdi.spark.session

import com.wangdi.util.{Constants, StringUtils}
import org.apache.spark.AccumulatorParam

/**
  * @ Author ：wang di
  * @ Date   ：Created in 2:21 PM 2018/11/12
  */
object SessionAggrStatAccumulator extends AccumulatorParam[String]{

  val serialVersionUID = 6311074555136039130L

  override def zero(v:String):String= Constants.SESSION_COUNT + "=0|" +
    Constants.TIME_PERIOD_1s_3s + "=0|" +
    Constants.TIME_PERIOD_4s_6s + "=0|" +
    Constants.TIME_PERIOD_7s_9s + "=0|" +
    Constants.TIME_PERIOD_10s_30s + "=0|" +
    Constants.TIME_PERIOD_30s_60s + "=0|" +
    Constants.TIME_PERIOD_1m_3m + "=0|" +
    Constants.TIME_PERIOD_3m_10m + "=0|" +
    Constants.TIME_PERIOD_10m_30m + "=0|" +
    Constants.TIME_PERIOD_30m + "=0|" +
    Constants.STEP_PERIOD_1_3 + "=0|" +
    Constants.STEP_PERIOD_4_6 + "=0|" +
    Constants.STEP_PERIOD_7_9 + "=0|" +
    Constants.STEP_PERIOD_10_30 + "=0|" +
    Constants.STEP_PERIOD_30_60 + "=0|" +
    Constants.STEP_PERIOD_60 + "=0"


  override def addInPlace(v1: String, v2: String): String = add(v1, v2)

  override def addAccumulator(v1: String, v2: String): String = add(v1, v2)

  private def add(v1: String, v2: String): String = { // 校验：v1为空的话，直接返回v2
    if (StringUtils.isEmpty(v1)) return v2
    // 使用StringUtils工具类，从v1中，提取v2对应的值，并累加1
    val oldValue = StringUtils.getFieldFromConcatString(v1, "\\|", v2)
    if (oldValue != null) { // 将范围区间原有的值，累加1
      val newValue = Integer.valueOf(oldValue) + 1
      // 使用StringUtils工具类，将v1中，v2对应的值，设置成新的累加后的值
      return StringUtils.setFieldInConcatString(v1, "\\|", v2, String.valueOf(newValue))
    }
    v1
  }
}
