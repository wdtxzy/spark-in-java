package com.wangdi.model;

/**
 * 切片转化率
 * @author : wangdi
 * @time : creat in 2018/11/20 10:26 AM
 */
public class PageSplitConvertRate {

    private long taskid;
    private String convertRate;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
