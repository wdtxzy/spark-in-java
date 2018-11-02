package com.wangdi.dao.impl;

import com.wangdi.dao.SessionRandomExtractDao;
import com.wangdi.model.SessionRandomExtract;
import com.wangdi.util.JDBCUtils;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:46 AM 2018/11/1
 */
public class SessionRandomExtractDaoImpl implements SessionRandomExtractDao {

    public void insert(SessionRandomExtract sessionRandomExtract){
        String sql = "insert into session_random_extract values(?,?,?,?,?)";

        Object[] params = new Object[]{sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
