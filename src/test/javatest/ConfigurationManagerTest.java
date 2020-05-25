/**
 * FileName: ConfigurationManagerTest
 * Author:   86155
 * Date:     2020/5/24 10:46
 * Description:
 */

package javatest;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;

public class ConfigurationManagerTest {
    public static void main(String[] args) {
        System.out.println(ConfigurationManager.getProperty("testkey"));
    }
}
