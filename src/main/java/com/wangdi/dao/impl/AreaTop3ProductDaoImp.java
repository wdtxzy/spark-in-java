package com.wangdi.dao.impl;

import com.wangdi.dao.AreaTop3ProductDao;
import com.wangdi.model.AreaTop3Product;
import com.wangdi.util.JDBCUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @author : wangdi
 * @time : creat in 2018/11/20 10:27 AM
 */
public class AreaTop3ProductDaoImp implements AreaTop3ProductDao {

    @Override
    public void insertBatch(List<AreaTop3Product> areaTopsProducts) {
        String sql = "INSERT INTO area_top3_product VALUES(?,?,?,?,?,?,?,?)";

        List<Object[]> paramsList = new ArrayList<Object[]>();

        for(AreaTop3Product areaTop3Product : areaTopsProducts) {
            Object[] params = new Object[8];

            params[0] = areaTop3Product.getTaskid();
            params[1] = areaTop3Product.getArea();
            params[2] = areaTop3Product.getAreaLevel();
            params[3] = areaTop3Product.getProductid();
            params[4] = areaTop3Product.getCityInfos();
            params[5] = areaTop3Product.getClickCount();
            params[6] = areaTop3Product.getProductName();
            params[7] = areaTop3Product.getProductStatus();

            paramsList.add(params);
        }

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeBatch(sql, paramsList);
    }
}
