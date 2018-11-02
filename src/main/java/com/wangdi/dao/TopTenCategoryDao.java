package com.wangdi.dao;

import com.wangdi.model.TopTenCategory;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:50 AM 2018/11/1
 * top10品类DAO接口
 */
public interface TopTenCategoryDao {

    //插入一条top10数据
    void insert(TopTenCategory topTenCategory);
}
