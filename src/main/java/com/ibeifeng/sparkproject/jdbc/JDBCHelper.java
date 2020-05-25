/**
 * FileName: JDBCHelper
 * Author:   86155
 * Date:     2020/5/24 15:47
 * Description:JDBC辅助组件
 */

package com.ibeifeng.sparkproject.jdbc;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

public class JDBCHelper {
    // 第一步：在静态代码块中，直接加载数据库的驱动
    static {
        try {
            String dirver = ConfigurationManager.getProperty(
                    Constants.JDBC_DRIVER);
            Class.forName(dirver);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    // 第二步，实现JDBCHelper的单例化
    private static JDBCHelper instance = null;
    public static JDBCHelper getInstance(){
        if (instance == null){
            synchronized (JDBCHelper.class){
                if (instance == null){
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }
    // 第三步：实现单例的过程中，创建唯一的数据库连接池
    private LinkedList<Connection> datasource = new LinkedList<Connection>();
    private JDBCHelper(){
        int datasourcesize = ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        for (int i = 0; i < datasourcesize; i++) {
            Connection conn = null;
            try {
                conn = DriverManager.getConnection(ConfigurationManager.getProperty(Constants.JDBC_URL),
                        ConfigurationManager.getProperty(Constants.JDBC_USER),
                        ConfigurationManager.getProperty(Constants.JDBC_PASSWORD));

                datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    // 第四步，提供获取数据库连接的方法
    public synchronized Connection getConnection(){
        if (datasource.size() == 0){
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasource.poll();
    }
    //第五步开发增删改的方法
    public int executeUpdate(String sql,Object[] params) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        int ret = 0;
        try {
            conn = JDBCHelper.getInstance().getConnection();
            pstmt = conn.prepareStatement(sql);

            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
            ret = pstmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
        return ret;
    }
    public int[] executeBatch(String sql, List<Object[]> params){
        Connection conn = null;
        PreparedStatement pstmt = null;
        int [] ret = null;

        try {
            conn = JDBCHelper.getInstance().getConnection();
            conn.setAutoCommit(false);

            pstmt = conn.prepareStatement(sql);
            for (Object[] param : params) {
                for (int i = 0; i <param.length ; i++) {
                    pstmt.setObject(i + 1, param[i]);
                }
                pstmt.addBatch();
            }
            ret = pstmt.executeBatch();
            conn.commit();
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if(conn != null){
                datasource.push(conn);
            }
        }
    return ret;
    }
    public void executeQuery(String sql,Object[] param,QueryCallback queryCallback){
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            conn = JDBCHelper.getInstance().getConnection();
            pstmt = conn.prepareStatement(sql);
            for (int i = 0; i <param.length ; i++) {
                pstmt.setObject(i+1,param[i]);
            }
            rs = pstmt.executeQuery();
            queryCallback.process(rs);
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            if (conn != null) {
                datasource.push(conn);
            }
        }
    }


    /**
     * 静态内部类：查询回调接口
     */
    public static interface QueryCallback {

        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;

    }
}
