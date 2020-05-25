/**
 * FileName: TaskDAOImpl
 * Author:   86155
 * Date:     2020/5/25 10:14
 * Description:
 */

package com.ibeifeng.sparkproject.dao.daoimpl;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.domain.Task;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;

public class TaskDAOImpl implements ITaskDAO {


    @Override
    public Task findById(long taskid) {

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String sql = "select * from task where task_id = ?";
        Object[] param = new Object[]{taskid};
        final Task task = new Task();
        jdbcHelper.executeQuery(sql, param, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()){
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
