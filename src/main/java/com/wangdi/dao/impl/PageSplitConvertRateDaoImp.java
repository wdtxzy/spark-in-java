package com.wangdi.dao.impl;

import com.wangdi.dao.PageSplitConvertRateDao;
import com.wangdi.model.PageSplitConvertRate;
import com.wangdi.util.JDBCUtils;

/**
 * @author : wangdi
 * @time : creat in 2018/11/20 10:31 AM
 */
public class PageSplitConvertRateDaoImp implements PageSplitConvertRateDao {

    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeUpdate(sql, params);
    }
}
