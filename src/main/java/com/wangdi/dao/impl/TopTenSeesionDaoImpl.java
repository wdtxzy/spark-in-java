package com.wangdi.dao.impl;

import com.wangdi.dao.TopTenSessionDao;
import com.wangdi.model.TopTenSession;
import com.wangdi.util.JDBCUtils;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:54 AM 2018/11/1
 */
public class TopTenSeesionDaoImpl implements TopTenSessionDao {

    public void insert(TopTenSession topTenSession){
        String sql = "insert into top10_session values(?,?,?,?)";

        Object[] params = new Object[]{topTenSession.getTaskid(),
                topTenSession.getCategoryid(),
                topTenSession.getSessionid(),
                topTenSession.getClickCount()};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
