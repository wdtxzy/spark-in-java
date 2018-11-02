package com.wangdi.dao;

import com.wangdi.model.TopTenSession;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:52 AM 2018/11/1
 * top10活跃的session的Dao接口
 */
public interface TopTenSessionDao {

    //插入一条top10活跃的session数据
    void insert(TopTenSession TopTenSession);
}
