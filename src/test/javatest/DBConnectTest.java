/**
 * FileName: DBConnectTest
 * Author:   86155
 * Date:     2020/5/24 14:50
 * Description:
 */

package javatest;


import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class DBConnectTest {
    public static void main(String[] args) throws Exception{
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        /*ArrayList<Object[]> params = new ArrayList<Object[]>();
        params.add(new Object[]{"wangwu"});
        params.add(new Object[]{"lisi"});
        //jdbcHelper.executeUpdate("insert into test(name) values(?)",new Object[]{"hxd"});
        jdbcHelper.executeBatch("insert into test(name) values(?)",
                params);*/
        final Map<String,Object> map = new HashMap<String, Object>();
        jdbcHelper.executeQuery("select * from test where id = ? ", new Object[]{1}, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                    int id = rs.getInt(1);
                    String name = rs.getString(2);
                    map.put("id",id);
                    map.put("name",name);
                }
            }
        });
        System.out.println(map.get("id") + ":" + map.get("name") );

    }
}
