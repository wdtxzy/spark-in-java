package com.wangdi.dao;

import com.wangdi.model.SessionDetail;

import java.util.List;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:40 AM 2018/11/1
 * Session明细接口
 */
public interface SessionDetailDao {

    //插入一条明细数据
    void insert(SessionDetail sessionDetail);

    //批量插入明细数据
    void insertBatch(List<SessionDetail> sessionDetailList);
}
