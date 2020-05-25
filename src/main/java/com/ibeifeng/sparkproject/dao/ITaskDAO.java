/**
 * FileName: ITaskDAO
 * Author:   86155
 * Date:     2020/5/25 10:11
 * Description:
 */

package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.Task;

public interface ITaskDAO {
    /**
     * 根据任务id查询任务
     * @param taskid
     * @return
     */
    Task findById(long taskid);
}
