/**
 * FileName: fanctory
 * Author:   86155
 * Date:     2020/5/25 10:21
 * Description:
 */

package com.ibeifeng.sparkproject.dao.factory;

import com.ibeifeng.sparkproject.dao.*;
import com.ibeifeng.sparkproject.dao.daoimpl.*;

public class DAOFactory {
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
    public static ISessionAggrStatDAO getSessionAggrStatDAO(){

        return new SessionAggrStatDAOImpl();
    }
    public static ISessionRandomExtractDAO getISessionRandomExtractDAO(){
        return new SessionRandomExtractDAOImpl();
    }
    public static ISessionDetailDAO getISessionDetailDAO(){
        return new SessionDetailDAOImpl();
    }
    public static ITop10CategoryDAO getITop10CategoryDAO(){
        return new Top10CategoryDAOImpl();
    }
    public static ITop10SessionDAO getITop10SessionDAO(){
        return new Top10SessionDAOImpl();
    }

}
