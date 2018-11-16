package com.wangdi.spark.session

/**
  * @ Author ：wang di
  * @ Date   ：Created in 10:48 AM 2018/11/15
  * 品类二次排序
  */
@SerialVersionUID(-6007890914324789180L)
case class CategorySortKey(var clickCount:Long,var orderCount:Long,var payCount:Long) extends Ordered[CategorySortKey] with Serializable{

  override def $greater(other: CategorySortKey): Boolean = {
    if (clickCount > other.getClickCount) return true
    else if (clickCount == other.getClickCount && orderCount > other.getOrderCount) return true
    else if (clickCount == other.getClickCount && orderCount == other.getOrderCount && payCount > other.getPayCount) return true
    false
  }

  override def $greater$eq(other: CategorySortKey): Boolean = {
    if ($greater(other)) return true
    else if (clickCount == other.getClickCount && orderCount == other.getOrderCount && payCount == other.getPayCount) return true
    false
  }

  override def $less(other: CategorySortKey): Boolean = {
    if (clickCount < other.getClickCount) return true
    else if (clickCount == other.getClickCount && orderCount < other.getOrderCount) return true
    else if (clickCount == other.getClickCount && orderCount == other.getOrderCount && payCount < other.getPayCount) return true
    false
  }

  override def $less$eq(other: CategorySortKey): Boolean = {
    if ($less(other)) return true
    else if (clickCount == other.getClickCount && orderCount == other.getOrderCount && payCount == other.getPayCount) return true
    false
  }

  override def compare(other: CategorySortKey): Int = {
    if (clickCount - other.getClickCount != 0) return (clickCount - other.getClickCount).toInt
    else if (orderCount - other.getOrderCount != 0) return (orderCount - other.getOrderCount).toInt
    else if (payCount - other.getPayCount != 0) return (payCount - other.getPayCount).toInt
    0
  }

  override def compareTo(other: CategorySortKey): Int = {
    if (clickCount - other.getClickCount != 0) return (clickCount - other.getClickCount).toInt
    else if (orderCount - other.getOrderCount != 0) return (orderCount - other.getOrderCount).toInt
    else if (payCount - other.getPayCount != 0) return (payCount - other.getPayCount).toInt
    0
  }

  def getClickCount: Long = clickCount

  def setClickCount(clickCount: Long): Unit = {
    this.clickCount = clickCount
  }

  def getOrderCount: Long = orderCount

  def setOrderCount(orderCount: Long): Unit = {
    this.orderCount = orderCount
  }

  def getPayCount: Long = payCount

  def setPayCount(payCount: Long): Unit = {
    this.payCount = payCount
  }
}


