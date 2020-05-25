/**
 * FileName: NumberUtils
 * Author:   86155
 * Date:     2020/5/24 14:11
 * Description: 数字格工具类
 */

package com.ibeifeng.sparkproject.util;

import java.math.BigDecimal;

public class NumberUtils {
    public static double formatDouble(double num,int scale){
        BigDecimal bd = new BigDecimal(num);
        return bd.setScale(scale,BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
