/**
 * FileName: fanctory
 * Author:   86155
 * Date:     2020/5/25 10:21
 * Description:
 */

package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.ITaskDAO;
import com.ibeifeng.sparkproject.dao.daoimpl.TaskDAOImpl;

public class DAOFactory {
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
