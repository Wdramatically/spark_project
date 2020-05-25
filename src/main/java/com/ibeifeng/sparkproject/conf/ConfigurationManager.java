/**
 * FileName: ConfigurationManager
 * Author:   hxd
 * Date:     2020/5/24 10:28
 * Description: 配置管理组件
 */

package com.ibeifeng.sparkproject.conf;

import java.io.InputStream;
import java.util.Properties;

public class ConfigurationManager {

    //将prop设置为类私有的变量，为了避免外界直接通过ConfigurationManager.prop来获取到properties
    //也就避免外界代码不小心错误地更新properties
    private static Properties prop = new Properties();

    /**
     * 静态代码块，只会在类加载时执行，也就是说我们的配置文件只会在该类加载时获取一次，就可以重复使用
     */
    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }
    public static Integer getInteger(String key) {
        try {
            return Integer.valueOf(prop.getProperty(key));
        }catch (Exception e){
            e.printStackTrace();
        }
        return 0;
    }
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }
}
