package com.wangdi.dao;

import com.wangdi.model.Task;

/**
 * @ Author ：wang di
 * @ Date   ：Created in 10:28 AM 2018/11/1
 * 任务管理DAO接口
 */
public interface TaskDao {

    //根据任务id查找任务
    Task findById(long taskid);
}
