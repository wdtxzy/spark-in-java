package com.wangdi.dao;

import com.wangdi.dao.impl.*;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:57 AM 2018/11/1
 * Dao 工厂类
 */
public class DaoFactory {

    public static TaskDao getTaskDAO() {
        return new TaskDaoImpl();
    }

    public static SessionAggrStatDao getSessionAggrStatDao() {
        return new SessionAggrStatDaoImpl();
    }

    public static SessionRandomExtractDao getSessionRandomExtractDao() {
        return new SessionRandomExtractDaoImpl();
    }

    public static SessionDetailDao getSessionDetailDao() {
        return new SessionDetailDaoImpl();
    }

    public static TopTenCategoryDao getTop10CategoryDao() {
        return new TopTenCategoryDaoImpl();
    }

    public static TopTenSessionDao getTop10SessionDao() {
        return new TopTenSeesionDaoImpl();
    }

    public static AreaTop3ProductDao getAreaTop3ProductDao(){return new AreaTop3ProductDaoImp();}

    public static PageSplitConvertRateDao getPageSplitConvertRateDao(){return new PageSplitConvertRateDaoImp();}
}
