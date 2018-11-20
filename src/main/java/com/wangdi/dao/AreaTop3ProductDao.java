package com.wangdi.dao;

import com.wangdi.model.AreaTop3Product;

import java.util.List;

/**
 * @author : wangdi
 * @time : creat in 2018/11/20 10:27 AM
 */
public interface AreaTop3ProductDao {

    void insertBatch(List<AreaTop3Product> areaTopsProducts);
}
