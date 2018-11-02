package com.wangdi.util;

import com.wangdi.conf.ConfigurationManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.LinkedList;
import java.util.List;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 9:57 AM 2018/11/1
 */
public class JDBCUtils {

    static {
        try {
            String driver = ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static JDBCUtils instance = null;

    //获取单例
    public static JDBCUtils getInstance(){
        if(instance == null) {
            synchronized(JDBCUtils.class) {
                if(instance == null) {
                    instance = new JDBCUtils();
                }
            }
        }
        return instance;
    }

    // 数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();

    //创建连接放到连接池中
    private JDBCUtils() {
        int datasourceSize = ConfigurationManager.getInteger(
                Constants.JDBC_DATASOURCE_SIZE);

        for(int i = 0; i < datasourceSize; i++) {
            boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
            String url = null;
            String user = null;
            String password = null;

            if(local) {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            } else {
                url = ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user = ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password = ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasource.push(conn);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    //获得链接
    public synchronized Connection getConnection(){
        while (datasource.size() == 0){
            try {
                Thread.sleep(10);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }

    //增加增删改查方法
    public int executeUpdate(String sql,Object[] params){
        int result = 0;
        Connection con = null;
        PreparedStatement pre = null;

        try {
            con = getConnection();
            con.setAutoCommit(false);
            pre = con.prepareStatement(sql);

            if(params != null && params.length>0){
                for(int i = 0;i<params.length;i++){
                    pre.setObject(i+1,params[i]);
                }
            }

            result = pre.executeUpdate();
            con.commit();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(con != null){
                datasource.push(con);
            }
        }
        return result;
    }

    public void executeQuery(String sql,Object[] params, QueryCallback callback){
        ResultSet result = null;
        Connection con = null;
        PreparedStatement pre = null;

        try {
            con = getConnection();
            con.setAutoCommit(false);
            pre = con.prepareStatement(sql);

            if(params != null && params.length>0){
                for(int i =0;i<params.length;i++){
                    pre.setObject(i+1,params[i]);
                }
            }
            result = pre.executeQuery();
            callback.process(result);

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(con != null){
                datasource.push(con);
            }
        }
    }

    public int[] executeBatch(String sql, List<Object[]> paramsList){
        int[] result = null;
        Connection con = null;
        PreparedStatement pre = null;

        try {
            con = getConnection();
            con.setAutoCommit(false);
            pre = con.prepareStatement(sql);

            if(paramsList != null&& paramsList.size() > 0){
                for(Object[] params : paramsList){
                    for (int i =0;i<params.length;i++){
                        pre.setObject(i+1,params[i]);
                    }
                    pre.addBatch();
                }
            }
            result = pre.executeBatch();
            con.commit();

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(con != null){
                datasource.push(con);
            }
        }
        return result;
    }

    public static interface QueryCallback {

        void process(ResultSet rs) throws Exception;

    }
}
