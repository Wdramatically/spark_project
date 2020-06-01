/**
 * FileName: Top10SessionDAOImpl
 * Author:   86155
 * Date:     2020/5/29 10:58
 * Description:
 */

package com.ibeifeng.sparkproject.dao.daoimpl;

import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

public class Top10SessionDAOImpl implements ITop10SessionDAO {

    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";
        Object[] param = {top10Session.getTaskid(),top10Session.getCategoryid(),top10Session.getSessionid(),top10Session.getClickCount()};
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,param);
    }
}
