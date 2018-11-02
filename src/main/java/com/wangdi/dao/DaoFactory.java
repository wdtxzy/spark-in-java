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

    public static SessionAggrStatDao getSessionAggrStatDAO() {
        return new SessionAggrStatDaoImpl();
    }

    public static SessionRandomExtractDao getSessionRandomExtractDAO() {
        return new SessionRandomExtractDaoImpl();
    }

    public static SessionDetailDao getSessionDetailDAO() {
        return new SessionDetailDaoImpl();
    }

    public static TopTenCategoryDao getTop10CategoryDAO() {
        return new TopTenCategoryDaoImpl();
    }

    public static TopTenSessionDao getTop10SessionDAO() {
        return new TopTenSeesionDaoImpl();
    }
}
