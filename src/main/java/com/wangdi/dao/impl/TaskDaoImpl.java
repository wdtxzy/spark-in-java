package com.wangdi.dao.impl;

import com.wangdi.dao.TaskDao;
import com.wangdi.model.Task;
import com.wangdi.util.JDBCUtils;

import java.sql.ResultSet;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:30 AM 2018/11/1
 */
public class TaskDaoImpl  implements TaskDao {

    public Task findById(long taskid){
        final Task task = new Task();

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[]{taskid};

        JDBCUtils jdbcUtils = JDBCUtils.getInstance();
        jdbcUtils.executeQuery(sql, params, new JDBCUtils.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    long taskid = rs.getLong(1);
                    String taskName = rs.getString(2);
                    String createTime = rs.getString(3);
                    String startTime = rs.getString(4);
                    String finishTime = rs.getString(5);
                    String taskType = rs.getString(6);
                    String taskStatus = rs.getString(7);
                    String taskParam = rs.getString(8);

                    task.setTaskid(taskid);
                    task.setTaskName(taskName);
                    task.setCreateTime(createTime);
                    task.setStartTime(startTime);
                    task.setFinishTime(finishTime);
                    task.setTaskType(taskType);
                    task.setTaskStatus(taskStatus);
                    task.setTaskParam(taskParam);
                }
            }
        });

        return task;
    }

}
