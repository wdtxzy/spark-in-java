package com.wangdi.dao.impl;

import com.wangdi.dao.SessionDetailDao;
import com.wangdi.model.SessionDetail;
import com.wangdi.util.JDBCUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:42 AM 2018/11/1
 */
public class SessionDetailDaoImpl  implements SessionDetailDao {

    public void insert(SessionDetail sessionDetail){
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        Object[] params = new Object[]{sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }

    public void insertBatch(List<SessionDetail> sessionDetails){
        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();
        for(SessionDetail sessionDetail : sessionDetails) {
            Object[] params = new Object[]{sessionDetail.getTaskid(),
                    sessionDetail.getUserid(),
                    sessionDetail.getSessionid(),
                    sessionDetail.getPageid(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickProductId(),
                    sessionDetail.getOrderCategoryIds(),
                    sessionDetail.getOrderProductIds(),
                    sessionDetail.getPayCategoryIds(),
                    sessionDetail.getPayProductIds()};
            paramsList.add(params);
        }

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeBatch(sql, paramsList);
    }
}
