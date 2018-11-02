package com.wangdi.util;


import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.wangdi.conf.ConfigurationManager;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 11:26 AM 2018/11/1
 */
public class ParamsUtils {

    //从命令行参数中提取任务id
    public static Long getTaskIdFromArgs(String[] args, String taskType) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);

        if(local) {
            return ConfigurationManager.getLong(taskType);
        } else {
            try {
                if(args != null && args.length > 0) {
                    return Long.valueOf(args[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return null;
    }

   //从JSON对象中提取参数
    public static String getParam(JSONObject jsonObject, String field) {
        JSONArray jsonArray = jsonObject.getJSONArray(field);
        if(jsonArray != null && jsonArray.size() > 0) {
            return jsonArray.getString(0);
        }
        return null;
    }
}
