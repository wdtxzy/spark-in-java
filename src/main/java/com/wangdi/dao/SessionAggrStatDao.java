package com.wangdi.dao;

import com.wangdi.model.SessionAggrStat;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:35 AM 2018/11/1
 * session 聚合统计模板DAO接口
 */
public interface SessionAggrStatDao {

    //插入session聚合结果
    void insert(SessionAggrStat sessionAggrStat);
}
