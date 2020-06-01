/**
 * FileName: Top10CategoryDAOImpl
 * Author:   86155
 * Date:     2020/5/28 15:44
 * Description:
 */

package com.ibeifeng.sparkproject.dao.daoimpl;

import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        long taskid = top10Category.getTaskid();
        long categoryid = top10Category.getCategoryid();
        long clickCount = top10Category.getClickCount();
        long orderCount = top10Category.getOrderCount();
        long payCount = top10Category.getPayCount();
        Object[] param ={taskid,categoryid,clickCount,orderCount,payCount};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,param);
    }
}
