package com.wangdi.dao.impl;

import com.wangdi.dao.TopTenCategoryDao;
import com.wangdi.model.TopTenCategory;
import com.wangdi.util.JDBCUtils;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:51 AM 2018/11/1
 */
public class TopTenCategoryDaoImpl implements TopTenCategoryDao {

    public void insert(TopTenCategory topTenCategory){
        String sql = "insert into top10_category values(?,?,?,?,?)";

        Object[] params = new Object[]{topTenCategory.getTaskid(),
                topTenCategory.getCategoryid(),
                topTenCategory.getClickCount(),
                topTenCategory.getOrderCount(),
                topTenCategory.getPayCount()};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
